package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.PersistenceResult;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static no.ssb.lds.api.persistence.Fragment.DELETED_MARKER;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.EMPTY_BYTE_ARRAY;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PRIMARY_INDEX;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.findAnyOneMatchingFragmentInPrimary;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.toTimestamp;

class ReadPublisher implements Flow.Publisher<PersistenceResult> {

    final FoundationDBPersistence persistence;
    final FoundationDBStatistics statistics;
    final Tuple snapshot;
    final String namespace;
    final String entity;
    final String id;

    final AtomicReference<Flow.Subscriber<? super PersistenceResult>> subscriberRef = new AtomicReference<>();
    final AtomicReference<DirectorySubspace> primaryRef = new AtomicReference<>();

    ReadPublisher(FoundationDBPersistence persistence, FoundationDBStatistics statistics, Tuple snapshot, String namespace, String entity, String id) {
        this.persistence = persistence;
        this.statistics = statistics;
        this.snapshot = snapshot;
        this.namespace = namespace;
        this.entity = entity;
        this.id = id;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super PersistenceResult> subscriber) {
        subscriberRef.set(subscriber);
        FoundationDBSubscription subscription = new FoundationDBSubscription(persistence.db, subscriber);
        subscription.registerFirstRequest(n -> doRead(subscription, n));
        subscriber.onSubscribe(subscription);
    }

    void doRead(FoundationDBSubscription subscription, long n) {
        DirectorySubspace primary = primaryRef.get();
        if (primary == null) {
            persistence.getPrimary(namespace, entity).thenAccept(p -> {
                primaryRef.set(p);
                doRead(subscription, n);
            });
            return;
        }

        /*
         * Determine the correct version timestamp of the document to use. Will perform a database access and fetch at most one key-value
         */
        findAnyOneMatchingFragmentInPrimary(statistics, subscription.transactionRef.get(), primary, id, snapshot).thenAccept(aMatchingKeyValue -> {
            Flow.Subscriber<? super PersistenceResult> subscriber = subscriberRef.get();

            if (aMatchingKeyValue == null) {
                // document not found
                subscriber.onNext(new PersistenceResult(Fragment.DONE, statistics));
                subscriber.onComplete();
            }
            if (!primary.contains(aMatchingKeyValue.getKey())) {
                subscriber.onNext(new PersistenceResult(Fragment.DONE, statistics));
                subscriber.onComplete();
            }
            Tuple key = primary.unpack(aMatchingKeyValue.getKey());
            Tuple version = key.getNestedTuple(1);
            String path = key.getString(2);
            if (DELETED_MARKER.equals(path)) {
                subscriber.onNext(new PersistenceResult(new Fragment(namespace, entity, id, toTimestamp(version), DELETED_MARKER, 0, EMPTY_BYTE_ARRAY), statistics));
                subscriber.onComplete();
            }

            /*
             * Get document with given version.
             */
            publishDocuments(subscription, snapshot, subscription.transactionRef.get(), statistics, primary, namespace, entity, id, version, 1);
        });
    }

    static void publishDocuments(FoundationDBSubscription subscription, Tuple snapshot, ReadTransaction transaction, FoundationDBStatistics statistics, DirectorySubspace primary, String namespace, String entity, String id, Tuple version, int limit) {
        AsyncIterable<KeyValue> range = transaction.getRange(primary.range(Tuple.from(id, version)));
        statistics.getRange(PRIMARY_INDEX);
        AsyncIterator<KeyValue> iterator = range.iterator();
        iterator.onHasNext().thenAccept(new PrimaryIterator(subscription, snapshot, statistics, namespace, entity, null, primary, iterator, limit));
    }
}
