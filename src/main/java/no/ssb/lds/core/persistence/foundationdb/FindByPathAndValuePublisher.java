package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.streaming.Fragment;

import java.util.ArrayList;
import java.util.TreeSet;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PATH_VALUE_INDEX;

class FindByPathAndValuePublisher implements Flow.Publisher<Fragment> {

    final FoundationDBPersistence persistence;
    final OrderedKeyValueTransaction transaction;
    final Tuple snapshot;
    final String namespace;
    final String entity;
    final String path;
    final byte[] value;
    final int limit;

    final AtomicReference<Throwable> failedPublisherThrowable = new AtomicReference<>();

    FindByPathAndValuePublisher(FoundationDBPersistence persistence, OrderedKeyValueTransaction transaction, Tuple snapshot, String namespace, String entity, String path, byte[] value, int limit) {
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
        this.path = path;
        this.value = value;
        this.limit = limit;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Fragment> subscriber) {
        FoundationDBSubscription subscription = new FoundationDBSubscription(subscriber);
        subscription.registerFirstRequest(() -> doFindByPathAndValue(subscription));
        subscription.onSubscribe();
        if (failedPublisherThrowable.get() != null) {
            subscriber.onError(failedPublisherThrowable.get());
        }
    }

    void doFindByPathAndValue(FoundationDBSubscription subscription) {
        persistence.getPrimary(namespace, entity).thenAccept(primary -> {
            String arrayIndexUnawarePath = Fragment.computeIndexUnawarePath(path, new ArrayList<>());
            persistence.getIndex(namespace, entity, arrayIndexUnawarePath).thenAccept(index -> {
                byte[] truncatedValue = Fragment.truncate(value);
                Range range = index.range(Tuple.from(truncatedValue));
                AsyncIterable<KeyValue> rangeIterable = transaction.getRange(range, PATH_VALUE_INDEX);
                AsyncIterator<KeyValue> rangeIterator = rangeIterable.iterator();
                PathValueIndexIterator pathValueIndexIterator = new PathValueIndexIterator(subscription, persistence, snapshot, transaction, rangeIterator, primary, index, new TreeSet<>(), namespace, entity, path, value, limit);
                rangeIterator.onHasNext().thenAccept(pathValueIndexIterator);
            });
        });
    }
}
