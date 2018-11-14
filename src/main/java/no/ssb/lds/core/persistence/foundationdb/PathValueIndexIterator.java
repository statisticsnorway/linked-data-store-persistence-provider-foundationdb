package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.buffered.Document;
import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.PersistenceResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static no.ssb.lds.api.persistence.Fragment.DELETED_MARKER;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PATH_VALUE_INDEX;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PRIMARY_INDEX;

class PathValueIndexIterator implements Consumer<Boolean> {

    final FoundationDBSubscription subscription;
    final FoundationDBPersistence persistence;
    final Tuple snapshot;
    final ReadTransaction transaction;
    final FoundationDBStatistics statistics;
    final AsyncIterator<KeyValue> rangeIterator;
    final DirectorySubspace primary;
    final DirectorySubspace index;
    final NavigableSet<Tuple> versions;
    final String namespace;
    final String entity;
    final String path;
    final String value;
    final int limit;

    final List<Document> documents = Collections.synchronizedList(new ArrayList<>());
    final List<CompletableFuture<Document>> primaryLookupFutures = Collections.synchronizedList(new ArrayList<>());

    final AtomicInteger indexMatches = new AtomicInteger(0);
    final AtomicReference<String> versionsId = new AtomicReference<>();

    final AtomicBoolean cancel = new AtomicBoolean(false);

    int fragmentsPublished = 0;

    PathValueIndexIterator(FoundationDBSubscription subscription, FoundationDBPersistence persistence, Tuple snapshot, ReadTransaction transaction, FoundationDBStatistics statistics, AsyncIterator<KeyValue> rangeIterator, DirectorySubspace primary, DirectorySubspace index, NavigableSet<Tuple> versions, String namespace, String entity, String path, String value, int limit) {
        this.subscription = subscription;
        this.persistence = persistence;
        this.transaction = transaction;
        this.snapshot = snapshot;
        this.statistics = statistics;
        this.rangeIterator = rangeIterator;
        this.primary = primary;
        this.index = index;
        this.versions = versions;
        this.namespace = namespace;
        this.entity = entity;
        this.path = path;
        this.value = value;
        this.limit = limit;
        subscription.registerCancel(v -> cancel.set(true));
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
            subscription.onComplete();
            return;
        }

        KeyValue kv = rangeIterator.next();
        statistics.rangeIteratorNext(PATH_VALUE_INDEX);

        if (indexMatches.get() >= limit) {
            rangeIterator.cancel();
            statistics.rangeIteratorCancel(PATH_VALUE_INDEX);
            subscription.onComplete();
            return;
        }

        if (kv.getValue().length > 0 && !value.equals(new String(kv.getValue(), StandardCharsets.UTF_8))) {
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
            for (Tuple version : versions.descendingSet()) {
                if (version.compareTo(snapshot) <= 0) {
                    indexMatches.incrementAndGet();
                    onIndexMatch(id, version).thenAccept(v -> {
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

    CompletableFuture<Void> onIndexMatch(String id, Tuple version) {
        return persistence.findAnyOneMatchingFragmentInPrimary(statistics, transaction, primary, id, snapshot).thenCompose(aMatchingFragmentKv -> {
            Tuple keyTuple = primary.unpack(aMatchingFragmentKv.getKey());
            Tuple versionTuple = keyTuple.getNestedTuple(1);
            String path = keyTuple.getString(2);
            if (!version.equals(versionTuple)) {
                return CompletableFuture.completedFuture(null); // false-positive index-match on older version
            }
            if (DELETED_MARKER.equals(path)) {
                // Version was overwritten in primary by a delete-marker, schedule task to remove index fragment.
                persistence.db.runAsync(trn -> {
                    trn.clear(aMatchingFragmentKv.getKey());
                    return CompletableFuture.completedFuture((Void) null);
                }).exceptionally(throwable -> {
                    throwable.printStackTrace();
                    return null;
                });
                return CompletableFuture.completedFuture(null);
            }

            // TODO detect false-positive due to either index-value-truncation or value occupying multiple key-value slots
            // TODO This requires buffering all key-value slots of given fragment and must be done outside the persistence-provider layer

            AsyncIterable<KeyValue> range = transaction.getRange(primary.range(Tuple.from(id, version)));
            statistics.getRange(PRIMARY_INDEX);
            AsyncIterator<KeyValue> iterator = range.iterator();
            PrimaryIterator primaryIterator = new PrimaryIterator(subscription, snapshot, statistics, namespace, entity, null, primary, iterator, limit);
            iterator.onHasNext().thenAccept(primaryIterator);
            return primaryIterator.doneSignal.thenApply(count -> (Void) null);
        });
    }

    void onHasNoMore() {
        for (Tuple version : versions.descendingSet()) {
            if (version.compareTo(snapshot) <= 0) {
                indexMatches.incrementAndGet();
                onIndexMatch(versionsId.get(), version).thenAccept(v -> {
                    signalComplete();
                });
                return;
            }
        }
        signalComplete();
    }

    private void signalComplete() {
        if (fragmentsPublished == 0) {
            // no elements in iterator
            subscription.onNext(new PersistenceResult(Fragment.DONE, statistics));
        }
        subscription.onComplete();
    }
}
