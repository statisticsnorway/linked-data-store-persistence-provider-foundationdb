package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Fragment;

import java.util.ArrayList;
import java.util.TreeSet;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PATH_VALUE_INDEX;

class FindPublisher implements Flow.Publisher<Fragment> {

    final FoundationDBPersistence persistence;
    final FoundationDBTransaction transaction;
    final Tuple snapshot;
    final String namespace;
    final String entity;
    final String path;
    final String value;
    final int limit;

    final AtomicReference<Flow.Subscriber<? super Fragment>> subscriberRef = new AtomicReference<>();

    FindPublisher(FoundationDBPersistence persistence, FoundationDBTransaction transaction, Tuple snapshot, String namespace, String entity, String path, String value, int limit) {
        this.persistence = persistence;
        this.transaction = transaction;
        this.snapshot = snapshot;
        this.namespace = namespace;
        this.entity = entity;
        this.path = path;
        this.value = value;
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
            String arrayIndexUnawarePath = Fragment.computeIndexUnawarePath(path, new ArrayList<>());
            persistence.getIndex(namespace, entity, arrayIndexUnawarePath).thenAccept(index -> {
                String truncatedValue = Fragment.truncate(value);
                AsyncIterable<KeyValue> range = transaction.getRange(index.range(Tuple.from(truncatedValue)), PATH_VALUE_INDEX);
                AsyncIterator<KeyValue> rangeIterator = range.iterator();
                rangeIterator.onHasNext().thenAccept(new PathValueIndexIterator(subscription, persistence, snapshot, transaction, rangeIterator, primary, index, new TreeSet<>(), namespace, entity, path, value, limit));
            });
        });
    }
}
