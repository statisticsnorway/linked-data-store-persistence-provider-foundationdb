package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.PersistenceResult;

import java.util.ArrayList;
import java.util.TreeSet;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PATH_VALUE_INDEX;

class FindPublisher implements Flow.Publisher<PersistenceResult> {

    final FoundationDBPersistence persistence;
    final FoundationDBStatistics statistics;
    final Tuple snapshot;
    final String namespace;
    final String entity;
    final String path;
    final String value;
    final int limit;

    final AtomicReference<Flow.Subscriber<? super PersistenceResult>> subscriberRef = new AtomicReference<>();

    FindPublisher(FoundationDBPersistence persistence, FoundationDBStatistics statistics, Tuple snapshot, String namespace, String entity, String path, String value, int limit) {
        this.persistence = persistence;
        this.statistics = statistics;
        this.snapshot = snapshot;
        this.namespace = namespace;
        this.entity = entity;
        this.path = path;
        this.value = value;
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
            String arrayIndexUnawarePath = Fragment.computeIndexUnawarePath(path, new ArrayList<>());
            persistence.getIndex(namespace, entity, arrayIndexUnawarePath).thenAccept(index -> {
                String truncatedValue = Fragment.truncate(value);
                AsyncIterable<KeyValue> range = subscription.transactionRef.get().getRange(index.range(Tuple.from(truncatedValue)));
                statistics.getRange(PATH_VALUE_INDEX);
                AsyncIterator<KeyValue> rangeIterator = range.iterator();
                rangeIterator.onHasNext().thenAccept(new PathValueIndexIterator(subscription, persistence, snapshot, subscription.transactionRef.get(), statistics, rangeIterator, primary, index, new TreeSet<>(), namespace, entity, path, value, limit));
            });
        });
    }
}
