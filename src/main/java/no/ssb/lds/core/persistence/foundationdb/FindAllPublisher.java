package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.streaming.Fragment;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PRIMARY_INDEX;

class FindAllPublisher implements Flow.Publisher<Fragment> {

    final FoundationDBPersistence persistence;
    final OrderedKeyValueTransaction transaction;
    final Tuple snapshot;
    final String namespace;
    final String entity;
    final int limit;

    final AtomicReference<Throwable> failedPublisherThrowable = new AtomicReference<>();

    FindAllPublisher(FoundationDBPersistence persistence, OrderedKeyValueTransaction transaction, Tuple snapshot, String namespace, String entity, int limit) {
        try {
            if (transaction == null) {
                throw new IllegalArgumentException("transaction cannot be null");
            }
        } catch (Throwable t) {
            failedPublisherThrowable.set(t);
        }
        this.persistence = persistence;
        this.transaction = transaction;
        this.snapshot = snapshot;
        this.namespace = namespace;
        this.entity = entity;
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
        persistence.getPrimary(namespace, entity).thenAccept(primary -> {
            AsyncIterator<KeyValue> iterator = transaction.getRange(primary.range(Tuple.from()), PRIMARY_INDEX).iterator();
            PrimaryIterator primaryIterator = new PrimaryIterator(subscription, snapshot, transaction, namespace, entity, null, primary, iterator, limit);
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
