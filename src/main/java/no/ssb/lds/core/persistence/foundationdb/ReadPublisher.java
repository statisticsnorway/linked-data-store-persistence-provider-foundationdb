package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.FragmentType;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.EMPTY_BYTE_ARRAY;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PRIMARY_INDEX;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.findAnyOneMatchingFragmentInPrimary;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.toTimestamp;

class ReadPublisher implements Flow.Publisher<Fragment> {

    final FoundationDBPersistence persistence;
    final OrderedKeyValueTransaction transaction;
    final Tuple snapshot;
    final String namespace;
    final String entity;
    final String id;

    final AtomicReference<Flow.Subscriber<? super Fragment>> subscriberRef = new AtomicReference<>();
    final AtomicReference<Subspace> subspaceRef = new AtomicReference<>();

    ReadPublisher(FoundationDBPersistence persistence, OrderedKeyValueTransaction transaction, Tuple snapshot, String namespace, String entity, String id) {
        this.persistence = persistence;
        this.transaction = transaction;
        this.snapshot = snapshot;
        this.namespace = namespace;
        this.entity = entity;
        this.id = id;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Fragment> subscriber) {
        subscriberRef.set(subscriber);
        FoundationDBSubscription subscription = new FoundationDBSubscription(subscriber);
        subscription.registerFirstRequest(n -> doRead(subscription, n));
        subscriber.onSubscribe(subscription);
    }

    void doRead(FoundationDBSubscription subscription, long n) {
        try {
            Subspace primary = subspaceRef.get();
            if (primary == null) {
                persistence.getPrimary(namespace, entity).thenAccept(p -> {
                    subspaceRef.set(p);
                    doRead(subscription, n);
                }).exceptionally(t -> {
                    subscription.onError(t);
                    return null;
                });
                return;
            }

            /*
             * Determine the correct version timestamp of the document to use. Will perform a database access and fetch at most one key-value
             */
            findAnyOneMatchingFragmentInPrimary(transaction, primary, id, snapshot).thenAccept(aMatchingKeyValue -> {
                Flow.Subscriber<? super Fragment> subscriber = subscriberRef.get();

                if (aMatchingKeyValue == null) {
                    // document not found
                    subscriber.onNext(Fragment.DONE_NOT_LIMITED);
                    subscriber.onComplete();
                }
                if (!primary.contains(aMatchingKeyValue.getKey())) {
                    subscriber.onNext(Fragment.DONE_NOT_LIMITED);
                    subscriber.onComplete();
                }
                Tuple key = primary.unpack(aMatchingKeyValue.getKey());
                Tuple version = key.getNestedTuple(1);
                FragmentType fragmentType = FragmentType.fromTypeCode(key.getBytes(3)[0]);
                if (FragmentType.DELETED.equals(fragmentType)) {
                    subscriber.onNext(new Fragment(namespace, entity, id, toTimestamp(version), "", FragmentType.DELETED, 0, EMPTY_BYTE_ARRAY));
                    subscriber.onComplete();
                }

                /*
                 * Get document with given version.
                 */
                publishDocuments(subscription, snapshot, transaction, primary, namespace, entity, id, version, Integer.MAX_VALUE);
            }).exceptionally(t -> {
                subscription.onError(t);
                return null;
            });
        } catch (Throwable t) {
            subscription.onError(t);
        }
    }

    static void publishDocuments(FoundationDBSubscription subscription, Tuple snapshot, OrderedKeyValueTransaction transaction, Subspace primary, String namespace, String entity, String id, Tuple version, int limit) {
        AsyncIterable<KeyValue> range = transaction.getRange(primary.range(Tuple.from(id, version)), PRIMARY_INDEX);
        AsyncIterator<KeyValue> iterator = range.iterator();
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
    }
}
