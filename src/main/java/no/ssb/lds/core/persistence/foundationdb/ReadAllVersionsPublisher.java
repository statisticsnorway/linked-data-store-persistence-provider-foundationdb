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

    final AtomicReference<Flow.Subscriber<? super Fragment>> subscriberRef = new AtomicReference<>();
    final AtomicReference<Subspace> subspaceRef = new AtomicReference<>();

    ReadAllVersionsPublisher(FoundationDBPersistence persistence, OrderedKeyValueTransaction transaction, String namespace, String entity, String id, int limit) {
        this.persistence = persistence;
        this.transaction = transaction;
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
         * Get all fragments of all versions.
         */
        AsyncIterator<KeyValue> iterator = transaction.getRange(primary.range(Tuple.from(id)), PRIMARY_INDEX).iterator();
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
    }
}
