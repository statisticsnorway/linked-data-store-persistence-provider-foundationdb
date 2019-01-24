package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.streaming.FragmentType;

import java.util.Arrays;
import java.util.NavigableSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PATH_VALUE_INDEX;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PRIMARY_INDEX;

class PathValueIndexIterator implements Consumer<Boolean> {

    final FoundationDBSubscription subscription;
    final FoundationDBPersistence persistence;
    final Tuple snapshot;
    final OrderedKeyValueTransaction transaction;
    final AsyncIterator<KeyValue> rangeIterator;
    final Subspace primary;
    final Subspace index;
    final NavigableSet<Tuple> versions;
    final String namespace;
    final String entity;
    final String path;
    final byte[] value;
    final int limit;

    final AtomicInteger indexMatches = new AtomicInteger(0);
    final AtomicReference<String> versionsId = new AtomicReference<>();

    final AtomicBoolean cancel = new AtomicBoolean(false);

    final AtomicInteger fragmentsPublished = new AtomicInteger(0);

    PathValueIndexIterator(FoundationDBSubscription subscription, FoundationDBPersistence persistence, Tuple snapshot, OrderedKeyValueTransaction transaction, AsyncIterator<KeyValue> rangeIterator, Subspace primary, Subspace index, NavigableSet<Tuple> versions, String namespace, String entity, String path, byte[] value, int limit) {
        this.subscription = subscription;
        this.persistence = persistence;
        this.snapshot = snapshot;
        this.transaction = transaction;
        this.rangeIterator = rangeIterator;
        this.primary = primary;
        this.index = index;
        this.versions = versions;
        this.namespace = namespace;
        this.entity = entity;
        this.path = path;
        this.value = value;
        this.limit = limit;
        subscription.registerCancel(() -> cancel.set(true));
    }

    @Override
    public void accept(Boolean hasNext) {
        try {
            if (hasNext) {
                onHasNext();
            } else {
                onHasNoMore();
            }
        } catch (Throwable t) {
            rangeIterator.cancel();
            subscription.onError(t);
        }
    }

    void onHasNext() {
        if (cancel.get()) {
            // honor external cancel signal
            rangeIterator.cancel();
            return;
        }

        KeyValue kv = rangeIterator.next();

        if (indexMatches.get() >= limit) {
            rangeIterator.cancel();
            return;
        }

        if (kv.getValue().length > 0 && !Arrays.equals(value, kv.getValue())) {
            // false-positive match due to value truncation
            rangeIterator.onHasNext().thenAccept(this);
            return;
        }

        Tuple key = index.unpack(kv.getKey());
        String dbId = key.getString(1);
        Tuple matchedVersion = key.getNestedTuple(2);

        final String id = versionsId.get();
        if (id != null && !dbId.equals(id)) {
            // other resource
            for (Tuple version : versions) {
                if (version.compareTo(snapshot) >= 0) {
                    indexMatches.incrementAndGet();
                    onIndexMatch(id, version).thenAccept(fragmentsPublished -> {
                        this.fragmentsPublished.addAndGet(fragmentsPublished);
                        versions.clear();
                        versionsId.set(dbId);
                        versions.add(matchedVersion);
                        rangeIterator.onHasNext().thenAccept(this);
                    });
                    return;
                }
            }
            versions.clear();
        }
        versionsId.set(dbId);
        versions.add(matchedVersion);

        rangeIterator.onHasNext().thenAccept(this);
    }

    CompletableFuture<Integer> onIndexMatch(String id, Tuple version) {
        return persistence.findAnyOneMatchingFragmentInPrimary(transaction, primary, id, snapshot).thenCompose(aMatchingFragmentKv -> {
            Tuple keyTuple = primary.unpack(aMatchingFragmentKv.getKey());
            Tuple versionTuple = keyTuple.getNestedTuple(1);
            FragmentType fragmentType = FragmentType.fromTypeCode(keyTuple.getBytes(3)[0]);
            if (!version.equals(versionTuple)) {
                return CompletableFuture.completedFuture(null); // false-positive index-match on older version
            }
            if (FragmentType.DELETED.equals(fragmentType)) {
                // Version was overwritten in primary by a delete-marker, remove index fragment asynchronously in separate transaction.
                persistence.transactionFactory().runAsyncInIsolatedTransaction(tx -> {
                    ((OrderedKeyValueTransaction) tx).clear(aMatchingFragmentKv.getKey(), PATH_VALUE_INDEX);
                    return null;
                }, false).exceptionally(throwable -> {
                    throwable.printStackTrace();
                    return null;
                });
                return CompletableFuture.completedFuture(null);
            }

            // NOTE It's possible to get false-positive due to either index-value-truncation or value occupying
            // multiple key-value slots. These must be discarded by client or the buffered persistence layer.

            AsyncIterable<KeyValue> range = transaction.getRange(primary.range(Tuple.from(id, version)), PRIMARY_INDEX);
            AsyncIterator<KeyValue> iterator = range.iterator();
            PrimaryIterator primaryIterator = new PrimaryIterator(subscription, snapshot, transaction, namespace, entity, null, primary, iterator, limit);
            subscription.queuePublicationRequest(() -> iterator.onHasNext().thenAccept(primaryIterator));
            return primaryIterator.doneSignal;
        });
    }

    void onHasNoMore() {
        for (Tuple version : versions) {
            if (version.compareTo(snapshot) >= 0) {
                indexMatches.incrementAndGet();
                onIndexMatch(versionsId.get(), version).thenAccept(v -> subscription.onComplete());
                return;
            }
        }
        subscription.onComplete();
    }
}
