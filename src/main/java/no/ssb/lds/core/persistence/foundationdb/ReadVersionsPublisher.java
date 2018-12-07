package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.streaming.Fragment;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PRIMARY_INDEX;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.findAnyOneMatchingFragmentInPrimary;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.tickBackward;

class ReadVersionsPublisher implements Flow.Publisher<Fragment> {

    final FoundationDBPersistence persistence;
    final OrderedKeyValueTransaction transaction;
    final Tuple snapshotFrom;
    final Tuple snapshotTo;
    final String namespace;
    final String entity;
    final String id;
    final int limit;

    final AtomicReference<Flow.Subscriber<? super Fragment>> subscriberRef = new AtomicReference<>();
    final AtomicReference<Subspace> subspaceRef = new AtomicReference<>();

    ReadVersionsPublisher(FoundationDBPersistence persistence, OrderedKeyValueTransaction transaction, Tuple snapshotFrom, Tuple snapshotTo, String namespace, String entity, String id, int limit) {
        this.persistence = persistence;
        this.transaction = transaction;
        this.snapshotFrom = snapshotFrom;
        this.snapshotTo = snapshotTo;
        this.namespace = namespace;
        this.entity = entity;
        this.id = id;
        this.limit = limit;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Fragment> subscriber) {
        subscriberRef.set(subscriber);
        FoundationDBSubscription subscription = new FoundationDBSubscription(subscriber);
        subscription.registerFirstRequest(n -> doReadVersions(subscription, n));
        subscriber.onSubscribe(subscription);
    }

    void doReadVersions(FoundationDBSubscription subscription, long n) {
        Subspace primary = subspaceRef.get();
        if (primary == null) {
            persistence.getPrimary(namespace, entity).thenAccept(p -> {
                subspaceRef.set(p);
                doReadVersions(subscription, n);
            });
            return;
        }

        /*
         * Determine the correct version timestamp of the document to use. Will perform a database access and fetch at most one key-value
         */
        findAnyOneMatchingFragmentInPrimary(transaction, primary, id, snapshotFrom).thenAccept(aMatchingKeyValue -> {
            Flow.Subscriber<? super Fragment> subscriber = subscriberRef.get();

            if (aMatchingKeyValue == null) {
                // document not found
                subscriber.onNext(Fragment.DONE_NOT_LIMITED);
                subscriber.onComplete();
            }
            if (!primary.contains(aMatchingKeyValue.getKey())) {
                subscriber.onNext(Fragment.DONE_NOT_LIMITED);
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
            AsyncIterator<KeyValue> iterator = transaction.getRange(
                    KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, tickBackward(snapshotTo)))),
                    KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, tickBackward(firstMatchingVersion)))),
                    PRIMARY_INDEX
            ).iterator();

            PrimaryIterator primaryIterator = new PrimaryIterator(subscription, null, transaction, namespace, entity, null, primary, iterator, limit);
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
