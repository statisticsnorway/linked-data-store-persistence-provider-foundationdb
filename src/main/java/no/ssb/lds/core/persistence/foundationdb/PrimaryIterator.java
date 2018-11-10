package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Document;
import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.PersistenceResult;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.DELETED_MARKER;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PRIMARY_INDEX;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.throwRuntimeExceptionIfError;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.toTimestamp;

class PrimaryIterator implements Consumer<Boolean> {

    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    final Tuple snapshot;
    final FoundationDBStatistics statistics;
    final String namespace;
    final String entity;
    final DirectorySubspace primary;
    final AsyncIterator<KeyValue> iterator;
    final CompletableFuture<PersistenceResult> result;
    final int limit;
    int size = 0;

    final Map<String, List<Document>> documentById = new LinkedHashMap<>();
    final NavigableMap<String, Fragment> fragments = new TreeMap<>();
    StringBuilder value = new StringBuilder();
    String fragmentsId;
    Tuple fragmentsVersion;
    String fragmentsPath;

    final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    final CharBuffer charBuffer = CharBuffer.allocate(1024);

    PrimaryIterator(Tuple snapshot, FoundationDBStatistics statistics, String namespace, String entity, DirectorySubspace primary, AsyncIterator<KeyValue> iterator, CompletableFuture<PersistenceResult> result, int limit) {
        this.snapshot = snapshot;
        this.statistics = statistics;
        this.namespace = namespace;
        this.entity = entity;
        this.primary = primary;
        this.iterator = iterator;
        this.result = result;
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

    private void onHasNext() {
        KeyValue kv = iterator.next();
        statistics.rangeIteratorNext(PRIMARY_INDEX);
        Tuple keyTuple = primary.unpack(kv.getKey());
        String dbId = keyTuple.getString(0);
        Tuple version = keyTuple.getNestedTuple(1);
        String path = keyTuple.getString(2);
        long offset = keyTuple.getLong(3);

        if (fragmentsId == null) {
            // first iteration only
            fragmentsId = dbId;
            fragmentsVersion = version;
            fragmentsPath = path;
        }

        if (!path.equals(fragmentsPath)) {
            finishDecodingAndAddFragment();
        }

        if (!version.equals(fragmentsVersion)) {
            // version in fragment has changed from previous iteration to this iteration

            boolean limitReached = produceRelevantDocumentFromFragments(dbId);

            if (limitReached) {
                signalResultComplete(true);
                return; // limit reached and there are more matching documents
            }

            fragments.clear();
            decoder.reset();
        }

        decode(kv.getValue(), false);

        fragmentsVersion = version;
        fragmentsPath = path;
        fragmentsId = dbId;

        iterator.onHasNext().thenAccept(this);
    }

    private void onHasNoMore() {
        if (fragmentsId == null) {
            // no elements in iterator
            signalResultComplete(false);
            return;
        }
        finishDecodingAndAddFragment();
        boolean limitedMatches = produceRelevantDocumentFromFragments(fragmentsId);
        signalResultComplete(limitedMatches);
    }

    private void signalResultComplete(boolean limitedMatches) {
        Collection<List<Document>> values = documentById.values();
        List<Document> matches = new ArrayList<>();
        for (List<Document> versions : values) {
            matches.addAll(versions);
        }
        result.complete(PersistenceResult.readResult(matches, limitedMatches, statistics));
    }

    private void decode(byte[] inputBytes, boolean endOfInput) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(inputBytes);
        for (; ; ) {
            CoderResult coderResult = decoder.decode(byteBuffer, charBuffer, endOfInput);

            throwRuntimeExceptionIfError(coderResult);

            this.value.append(charBuffer.flip());

            charBuffer.clear(); // flush output buffer

            if (coderResult.isUnderflow()) {
                break; // read all of byteBuffer
            }

            // overflow
            nop();
        }
    }

    private void finishDecodingAndAddFragment() {
        decode(EMPTY_BYTE_ARRAY, true);
        decoder.flush(charBuffer);
        decoder.reset();
        fragments.put(fragmentsPath, new Fragment(fragmentsPath, value.toString()));
        value = new StringBuilder();
    }

    private boolean produceRelevantDocumentFromFragments(String dbId) {
        // produce a document of all the previous fragments that belonged to the same versioned resource
        if (!fragmentsId.equals(dbId)) {
            // next resource
            if (size >= limit) {
                iterator.cancel();
                statistics.rangeIteratorCancel(PRIMARY_INDEX);
                return true;
            }
            produceDocument(true);

        } else if (snapshot == null) {
            // next version (same resource) and snapshot is undefined (all versions wanted)
            produceDocument(false);

        } else if (fragmentsVersion.compareTo(snapshot) <= 0) {
            // next version (same resource) and version is older than or same as snapshot
            produceDocument(true);

        } else {
            // next version (same resource) and version is newer than snapshot
            nop();
        }

        return false;
    }

    private void produceDocument(boolean overwriteVersion) {
        Document document = createDocument(fragmentsId);
        List<Document> documents = documentById.computeIfAbsent(fragmentsId, id -> new ArrayList<>());
        if (documents.isEmpty()) {
            documents.add(document);
            size++;
        } else if (overwriteVersion) {
            documents.set(0, document);
        } else {
            documents.add(document);
            size++;
        }
    }

    private Document createDocument(String id) {
        if (DELETED_MARKER.equals(fragments.firstKey())) {
            return new Document(namespace, entity, id, toTimestamp(fragmentsVersion), Collections.emptyMap(), true);
        }
        return new Document(namespace, entity, id, toTimestamp(fragmentsVersion), Map.copyOf(fragments), false);
    }

    private void nop() {
    }
}