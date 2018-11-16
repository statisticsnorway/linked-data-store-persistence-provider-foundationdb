package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.PersistenceResult;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PRIMARY_INDEX;

class ReadAllVersionsPublisher implements Flow.Publisher<PersistenceResult> {

    final FoundationDBPersistence persistence;
    final FoundationDBStatistics statistics;
    final String namespace;
    final String entity;
    final String id;
    final int limit;

    final AtomicReference<Flow.Subscriber<? super PersistenceResult>> subscriberRef = new AtomicReference<>();
    final AtomicReference<DirectorySubspace> primaryRef = new AtomicReference<>();

    ReadAllVersionsPublisher(FoundationDBPersistence persistence, FoundationDBStatistics statistics, String namespace, String entity, String id, int limit) {
        this.persistence = persistence;
        this.statistics = statistics;
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
         * Get all fragments of all versions.
         */
        AsyncIterator<KeyValue> iterator = subscription.transactionRef.get().getRange(primary.range(Tuple.from(id))).iterator();
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
    }
}
