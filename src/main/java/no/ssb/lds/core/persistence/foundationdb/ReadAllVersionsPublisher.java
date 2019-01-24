package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.streaming.Fragment;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PRIMARY_INDEX;

class ReadAllVersionsPublisher implements Flow.Publisher<Fragment> {

    final FoundationDBPersistence persistence;
    final OrderedKeyValueTransaction transaction;
    final String namespace;
    final String entity;
    final String id;
    final int limit;

    final AtomicReference<Throwable> failedPublisherThrowable = new AtomicReference<>();
    final AtomicReference<Subspace> subspaceRef = new AtomicReference<>();

    ReadAllVersionsPublisher(FoundationDBPersistence persistence, OrderedKeyValueTransaction transaction, String namespace, String entity, String id, int limit) {
        try {
            if (transaction == null) {
                throw new IllegalArgumentException("transaction cannot be null");
            }
        } catch (Throwable t) {
            failedPublisherThrowable.set(t);
        }
        this.persistence = persistence;
        this.transaction = transaction;
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
         * Get all fragments of all versions.
         */
        AsyncIterator<KeyValue> iterator = transaction.getRange(primary.range(Tuple.from(id)), PRIMARY_INDEX).iterator();
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
    }
}
