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

    final AtomicReference<Throwable> failedPublisherThrowable = new AtomicReference<>();

    final AtomicReference<Subspace> subspaceRef = new AtomicReference<>();

    ReadVersionsPublisher(FoundationDBPersistence persistence, OrderedKeyValueTransaction transaction, Tuple snapshotFrom, Tuple snapshotTo, String namespace, String entity, String id, int limit) {
        try {
            if (transaction == null) {
                throw new IllegalArgumentException("transaction cannot be null");
            }
        } catch (Throwable t) {
            failedPublisherThrowable.set(t);
        }
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
        FoundationDBSubscription subscription = new FoundationDBSubscription(subscriber);
        subscription.registerFirstRequest(() -> doReadVersions(subscription));
        subscription.onSubscribe();
        if (failedPublisherThrowable.get() != null) {
            subscriber.onError(failedPublisherThrowable.get());
        }
    }

    void doReadVersions(FoundationDBSubscription subscription) {
        Subspace primary = subspaceRef.get();
        if (primary == null) {
            persistence.getPrimary(namespace, entity).thenAccept(p -> {
                subspaceRef.set(p);
                doReadVersions(subscription);
            });
            return;
        }

        /*
         * Determine the correct version timestamp of the document to use. Will perform a database access and fetch at most one key-value
         */
        findAnyOneMatchingFragmentInPrimary(transaction, primary, id, snapshotFrom).thenAccept(aMatchingKeyValue -> {

            Tuple lowerBound;
            if (aMatchingKeyValue == null || !primary.contains(aMatchingKeyValue.getKey())) {
                // no versions older or equals to snapshotFrom exists, use snapshotFrom as lower bound
                lowerBound = snapshotFrom;
            } else {
                Tuple key = primary.unpack(aMatchingKeyValue.getKey());
                lowerBound = tickBackward(key.getNestedTuple(1));
            }

            /*
             * Get document with given version.
             */
            /*
             * Get all fragments of all matching versions.
             */
            AsyncIterator<KeyValue> iterator = transaction.getRange(
                    KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, tickBackward(snapshotTo)))),
                    KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, lowerBound))),
                    PRIMARY_INDEX
            ).iterator();

            PrimaryIterator primaryIterator = new PrimaryIterator(subscription, null, transaction, namespace, entity, null, primary, iterator, limit);
            subscription.queuePublicationRequest(() -> iterator.onHasNext().thenAccept(primaryIterator));
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
