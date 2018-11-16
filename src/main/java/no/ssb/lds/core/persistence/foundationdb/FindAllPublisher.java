package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Fragment;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PRIMARY_INDEX;

class FindAllPublisher implements Flow.Publisher<Fragment> {

    final FoundationDBPersistence persistence;
    final FoundationDBTransaction transaction;
    final Tuple snapshot;
    final String namespace;
    final String entity;
    final int limit;

    final AtomicReference<Flow.Subscriber<? super Fragment>> subscriberRef = new AtomicReference<>();

    FindAllPublisher(FoundationDBPersistence persistence, FoundationDBTransaction transaction, Tuple snapshot, String namespace, String entity, int limit) {
        this.persistence = persistence;
        this.transaction = transaction;
        this.snapshot = snapshot;
        this.namespace = namespace;
        this.entity = entity;
        this.limit = limit;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Fragment> subscriber) {
        subscriberRef.set(subscriber);
        FoundationDBSubscription subscription = new FoundationDBSubscription(persistence.db, subscriber);
        subscription.registerFirstRequest(n -> doReadVersions(subscription, n));
        subscriber.onSubscribe(subscription);
    }

    void doReadVersions(FoundationDBSubscription subscription, long n) {
        persistence.getPrimary(namespace, entity).thenAccept(primary -> {
            AsyncIterator<KeyValue> iterator = transaction.getRange(primary.range(Tuple.from()), PRIMARY_INDEX).iterator();
            PrimaryIterator primaryIterator = new PrimaryIterator(subscription, snapshot, transaction, namespace, entity, null, primary, iterator, limit);
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
