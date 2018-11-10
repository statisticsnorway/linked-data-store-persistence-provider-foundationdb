package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Document;
import no.ssb.lds.api.persistence.PersistenceResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.DELETED_MARKER;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PATH_VALUE_INDEX;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.toTimestamp;

class PathValueIndexIterator implements Consumer<Boolean> {
    final FoundationDBPersistence persistence;

    final Tuple snapshot;
    final ReadTransaction transaction;
    final CompletableFuture<PersistenceResult> result;
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

    PathValueIndexIterator(FoundationDBPersistence persistence, Tuple snapshot, ReadTransaction transaction, CompletableFuture<PersistenceResult> result, FoundationDBStatistics statistics, AsyncIterator<KeyValue> rangeIterator, DirectorySubspace primary, DirectorySubspace index, NavigableSet<Tuple> versions, String namespace, String entity, String path, String value, int limit) {
        this.persistence = persistence;
        this.transaction = transaction;
        this.snapshot = snapshot;
        this.result = result;
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
            result.completeExceptionally(t);
        }
    }

    void onHasNext() {
        KeyValue kv = rangeIterator.next();
        statistics.rangeIteratorNext(PATH_VALUE_INDEX);

        if (indexMatches.get() >= limit) {
            rangeIterator.cancel();
            statistics.rangeIteratorCancel(PATH_VALUE_INDEX);
            result.complete(PersistenceResult.readResult(List.copyOf(documents), true, statistics));
            return;
        }

        if (kv.getValue().length > 0 && !value.equals(new String(kv.getValue(), StandardCharsets.UTF_8))) {
            // false-positive match due to value truncation
            rangeIterator.onHasNext().thenAccept(this);
            return;
        }

        Tuple key = index.unpack(kv.getKey());
        final String dbId = key.getString(1);

        final String id = versionsId.get();
        if (id != null && !dbId.equals(id)) {
            // other resource
            for (Tuple version : versions.descendingSet()) {
                if (version.compareTo(snapshot) <= 0) {
                    indexMatches.incrementAndGet();
                    primaryLookupFutures.add(onIndexMatch(id, version));
                    break;
                }
            }
            versions.clear();
        }
        versionsId.set(dbId);

        Tuple matchedVersion = key.getNestedTuple(2);
        versions.add(matchedVersion);

        rangeIterator.onHasNext().thenAccept(this);
    }

    CompletableFuture<Document> onIndexMatch(String id, Tuple version) {
        return persistence.findAnyOneMatchingFragmentInPrimary(statistics, transaction, primary, id, snapshot).thenCompose(aMatchingFragmentKv -> {
            Tuple keyTuple = primary.unpack(aMatchingFragmentKv.getKey());
            Tuple versionTuple = keyTuple.getNestedTuple(1);
            if (!version.equals(versionTuple)) {
                return null; // false-positive index-match on older version
            }
            if (DELETED_MARKER.equals(keyTuple.getString(2))) {
                // Version was overwritten in primary by a delete-marker, schedule task to remove index fragment.
                persistence.db.runAsync(trn -> doClearKeyValue(trn, aMatchingFragmentKv)).exceptionally(throwable -> {
                    throwable.printStackTrace();
                    return null;
                });
                return CompletableFuture.completedFuture(new Document(namespace, entity, id, toTimestamp(versionTuple), Collections.emptyMap(), true));
            }
            return persistence.getDocument(snapshot, transaction, statistics, namespace, entity, id, versionTuple, 1).thenApply(doc -> {
                if (doc == null) {
                    persistence.db.runAsync(trn -> doClearKeyValue(trn, aMatchingFragmentKv)).exceptionally(throwable -> {
                        throwable.printStackTrace();
                        return null;
                    });
                    return null;
                }
                if (!value.equals(doc.getFragmentByPath().get(path).getValue())) {
                    // false-positive due to either index-value-truncation or value occupying multiple key-value slots
                    return null;
                }
                documents.add(doc);
                return doc;
            });
        });
    }

    void onHasNoMore() {
        for (Tuple version : versions.descendingSet()) {
            if (version.compareTo(snapshot) <= 0) {
                indexMatches.incrementAndGet();
                primaryLookupFutures.add(onIndexMatch(versionsId.get(), version));
                break;
            }
        }
        CompletableFuture.allOf(primaryLookupFutures.toArray(CompletableFuture[]::new)).join();
        result.complete(PersistenceResult.readResult(List.copyOf(documents), false, statistics));
    }

    private CompletableFuture<PersistenceResult> doClearKeyValue(Transaction trn, KeyValue aMatchingFragmentKv) {
        FoundationDBStatistics stat = new FoundationDBStatistics();
        trn.clear(aMatchingFragmentKv.getKey());
        stat.clearKeyValue(PATH_VALUE_INDEX);
        return CompletableFuture.completedFuture(PersistenceResult.writeResult(stat));
    }

}
