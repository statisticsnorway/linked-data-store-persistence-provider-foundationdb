package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.PersistenceResult;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PRIMARY_INDEX;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.findAnyOneMatchingFragmentInPrimary;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.tickBackward;

class ReadVersionsPublisher implements Flow.Publisher<PersistenceResult> {

    final FoundationDBPersistence persistence;
    final FoundationDBStatistics statistics;
    final Tuple snapshotFrom;
    final Tuple snapshotTo;
    final String namespace;
    final String entity;
    final String id;
    final int limit;

    final AtomicReference<Flow.Subscriber<? super PersistenceResult>> subscriberRef = new AtomicReference<>();
    final AtomicReference<DirectorySubspace> primaryRef = new AtomicReference<>();

    ReadVersionsPublisher(FoundationDBPersistence persistence, FoundationDBStatistics statistics, Tuple snapshotFrom, Tuple snapshotTo, String namespace, String entity, String id, int limit) {
        this.persistence = persistence;
        this.statistics = statistics;
        this.snapshotFrom = snapshotFrom;
        this.snapshotTo = snapshotTo;
        this.namespace = namespace;
        this.entity = entity;
        this.id = id;
        this.limit = limit;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super PersistenceResult> subscriber) {
        subscriberRef.set(subscriber);
        FoundationDBSubscription subscription = new FoundationDBSubscription(persistence.db, subscriber);
        subscription.registerFirstRequest(n -> doReadVersions(subscription, n));
        subscriber.onSubscribe(subscription);
    }

    void doReadVersions(FoundationDBSubscription subscription, long n) {
        DirectorySubspace primary = primaryRef.get();
        if (primary == null) {
            persistence.getPrimary(namespace, entity).thenAccept(p -> {
                primaryRef.set(p);
                doReadVersions(subscription, n);
            });
            return;
        }

        /*
         * Determine the correct version timestamp of the document to use. Will perform a database access and fetch at most one key-value
         */
        findAnyOneMatchingFragmentInPrimary(statistics, subscription.transactionRef.get(), primary, id, snapshotFrom).thenAccept(aMatchingKeyValue -> {
            Flow.Subscriber<? super PersistenceResult> subscriber = subscriberRef.get();

            if (aMatchingKeyValue == null) {
                // document not found
                subscriber.onNext(new PersistenceResult(Fragment.DONE, statistics, false));
                subscriber.onComplete();
            }
            if (!primary.contains(aMatchingKeyValue.getKey())) {
                subscriber.onNext(new PersistenceResult(Fragment.DONE, statistics, false));
                subscriber.onComplete();
            }
            Tuple key = primary.unpack(aMatchingKeyValue.getKey());
            Tuple firstMatchingVersion = key.getNestedTuple(1);

            /*
             * Get document with given version.
             */
            /*
             * Get all fragments of all matching versions.
             */
            AsyncIterator<KeyValue> iterator = subscription.transactionRef.get().getRange(
                    KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, tickBackward(snapshotTo)))),
                    KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, tickBackward(firstMatchingVersion))))
            ).iterator();
            statistics.getRange(PRIMARY_INDEX);

            PrimaryIterator primaryIterator = new PrimaryIterator(subscription, null, statistics, namespace, entity, null, primary, iterator, limit);
            iterator.onHasNext().thenAccept(primaryIterator);
            primaryIterator.doneSignal
                    .thenAccept(fragmentsPublished -> {
                        subscription.onComplete();
                    })
                    .exceptionally(t -> {
                        subscription.onError(t);
                        return null;
                    });

        });
    }
}
