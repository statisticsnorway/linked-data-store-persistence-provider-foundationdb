package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.PersistenceResult;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PRIMARY_INDEX;

class FindAllPublisher implements Flow.Publisher<PersistenceResult> {

    final FoundationDBPersistence persistence;
    final FoundationDBStatistics statistics;
    final Tuple snapshot;
    final String namespace;
    final String entity;
    final int limit;

    final AtomicReference<Flow.Subscriber<? super PersistenceResult>> subscriberRef = new AtomicReference<>();

    FindAllPublisher(FoundationDBPersistence persistence, FoundationDBStatistics statistics, Tuple snapshot, String namespace, String entity, int limit) {
        this.persistence = persistence;
        this.statistics = statistics;
        this.snapshot = snapshot;
        this.namespace = namespace;
        this.entity = entity;
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
        persistence.getPrimary(namespace, entity).thenAccept(primary -> {
            AsyncIterator<KeyValue> iterator = subscription.transactionRef.get().getRange(primary.range(Tuple.from())).iterator();
            statistics.getRange(PRIMARY_INDEX);
            iterator.onHasNext().thenAccept(new PrimaryIterator(subscription, snapshot, statistics, namespace, entity, null, primary, iterator, limit));
        });
    }
}
