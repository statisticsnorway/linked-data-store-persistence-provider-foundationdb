package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Document;
import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.PersistenceResult;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class FoundationDBPersistence implements Persistence {

    static final String DELETED_MARKER = "DELETED";
    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    static final ZoneId ZONE_ID_UTC = ZoneId.of("Etc/UTC");
    static final int CHUNK_SIZE = 8 * 1024; // FoundationDB hard-limit is 100 KB.
    static final int MAX_DESIRED_KEY_LENGTH = 256; // FoundationDB hard-limit is 10 KB.

    static final String PRIMARY_INDEX = "Primary";
    static final String PATH_VALUE_INDEX = "PathValueIndex";

    final Database db;
    final Directory directory;
    final Map<Tuple, DirectorySubspace> directorySubspaceByPaths = new ConcurrentHashMap<>();

    public FoundationDBPersistence(Database db, Directory directory) {
        this.db = db;
        this.directory = directory;
    }

    /**
     * Directory: (NAMESPACE, "Primary", ENTITY)
     * Primary: (ID, (TIMESTAMP), PATH, OFFSET)  =  VALUE
     *
     * @param namespace
     * @param entity
     * @return
     */
    DirectorySubspace getPrimary(String namespace, String entity) {
        return createOrOpenDirectorySubspace(Tuple.from(namespace, PRIMARY_INDEX, entity));
    }

    /**
     * Directory: (NAMESPACE, "PathIndex", ENTITY, ARRAY-INDEX-UNAWARE-PATH)
     * PathIndex: (VALUE, ID, (TIMESTAMP), ARRAY-INDICES-FROM-PATH) = EMPTY
     *
     * @param namespace
     * @param entity
     * @param path
     * @return
     */
    DirectorySubspace getIndex(String namespace, String entity, String path) {
        return createOrOpenDirectorySubspace(Tuple.from(namespace, PATH_VALUE_INDEX, entity, path));
    }

    DirectorySubspace createOrOpenDirectorySubspace(Tuple key) {
        // To create a nested subdirectory per tuple item, use: directory.createOrOpen(db, t.stream().map(o -> (String) o).collect(Collectors.toList())
        return directorySubspaceByPaths.computeIfAbsent(key, t -> directory.createOrOpen(db, List.of(t.toString())).join());
    }

    @Override
    public CompletableFuture<PersistenceResult> createOrOverwrite(Document document) throws PersistenceException {
        return db.runAsync(transaction -> doCreateOrOverwrite(transaction, new FoundationDBStatistics(), document));
    }

    CompletableFuture<PersistenceResult> doCreateOrOverwrite(Transaction transaction, FoundationDBStatistics statistics, Document document) {
        DirectorySubspace primary = getPrimary(document.getNamespace(), document.getEntity());

        Tuple version = toTuple(document.getTimestamp());

        // Clear primary of existing document with same version
        transaction.clear(primary.range(Tuple.from(document.getId(), version)));
        statistics.clearRange(PRIMARY_INDEX);

        // NOTE: With current implementation we do not need to clear the index. False-positive matches in the index
        // are always followed up by a primary lookup. Clearing Index space is expensive as it requires a read to
        // figure out whether there is anything to clear and then another read to get existing doument and then finally
        // clearing each document fragment independently from the existing document in the index space which cannot be
        // done with a single range operation and therefore must be done using individual write operations per fragment.


        for (Fragment fragment : document.getFragmentByPath().values()) {

            /*
             * PRIMARY
             */

            CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
            CharBuffer charBuffer = CharBuffer.wrap(fragment.getValue());

            ByteBuffer byteBuffer = ByteBuffer.allocate(CHUNK_SIZE);
            for (int chunk = 0; ; chunk++) {
                Tuple primaryKey = Tuple.from(
                        document.getId(),
                        version,
                        fragment.getPath(),
                        chunk * CHUNK_SIZE
                );
                byte[] binaryPrimaryKey = primary.pack(primaryKey);
                CoderResult coderResult = encoder.encode(charBuffer, byteBuffer, false);

                throwRuntimeExceptionIfError(coderResult);

                if (coderResult.isOverflow()) {
                    transaction.set(binaryPrimaryKey, byteBuffer.array());
                    statistics.setKeyValue(PRIMARY_INDEX);
                    byteBuffer.clear();
                } else if (coderResult.isUnderflow()) {
                    CoderResult endOfInputEncodeResult = encoder.encode(charBuffer, byteBuffer, true);
                    if (!endOfInputEncodeResult.isUnderflow()) {
                        throw new RuntimeException("encoder endOfInput is not underflow");
                    }
                    CoderResult flushResult = encoder.flush(byteBuffer);
                    if (!flushResult.isUnderflow()) {
                        throw new RuntimeException("encoder flush is not underflow");
                    }
                    byteBuffer.flip();
                    byte[] value = new byte[byteBuffer.limit()];
                    byteBuffer.get(value);
                    transaction.set(binaryPrimaryKey, value);
                    statistics.setKeyValue(PRIMARY_INDEX);
                    break; // all chunks set
                }
            }

            /*
             * INDEX
             */
            String truncatedValue = truncateToMaxKeyLength(fragment.getValue());
            Tuple valueIndexKey = Tuple.from(
                    truncatedValue,
                    document.getId(),
                    version,
                    Tuple.from(fragment.getArrayIndices())
            );
            DirectorySubspace index = getIndex(document.getNamespace(), document.getEntity(), fragment.getArrayIndicesUnawarePath());
            byte[] binaryValueIndexKey = index.pack(valueIndexKey);
            if (binaryValueIndexKey.length > MAX_DESIRED_KEY_LENGTH) {
                throw new IllegalArgumentException("Document fragment key is too big for index, at most " + MAX_DESIRED_KEY_LENGTH + " bytes allowed. Was: " + binaryValueIndexKey.length + " bytes.");
            }
            transaction.set(binaryValueIndexKey, EMPTY_BYTE_ARRAY);
            statistics.setKeyValue(PATH_VALUE_INDEX);

        }

        return CompletableFuture.completedFuture(PersistenceResult.writeResult(statistics));
    }

    static String truncateToMaxKeyLength(String input) {
        return input.substring(0, Math.min(input.length(), MAX_DESIRED_KEY_LENGTH - 200));
    }

    @Override
    public CompletableFuture<PersistenceResult> read(ZonedDateTime snapshot, String namespace, String entity, String id) throws PersistenceException {
        return db.readAsync(transaction -> doRead(transaction, new FoundationDBStatistics(), toTuple(snapshot), namespace, entity, id));
    }

    CompletableFuture<PersistenceResult> doRead(ReadTransaction transaction, FoundationDBStatistics statistics, Tuple snapshot, String namespace, String entity, String id) {
        DirectorySubspace primary = getPrimary(namespace, entity);

        /*
         * Determine the correct version timestamp of the document to use. Will perform a database access and fetch at most one key-value
         */
        return findAnyOneMatchingFragmentInPrimary(statistics, transaction, primary, id, snapshot).thenCompose(aMatchingKeyValue -> {
            if (aMatchingKeyValue == null) {
                // document not found
                return CompletableFuture.completedFuture(PersistenceResult.readResult(Collections.emptyList(), false, statistics));
            }
            if (!primary.contains(aMatchingKeyValue.getKey())) {
                return CompletableFuture.completedFuture(PersistenceResult.readResult(Collections.emptyList(), false, statistics));
            }
            Tuple key = primary.unpack(aMatchingKeyValue.getKey());
            Tuple version = key.getNestedTuple(1);
            if (DELETED_MARKER.equals(key.getString(2))) {
                return CompletableFuture.completedFuture(PersistenceResult.readResult(new Document(namespace, entity, id, toTimestamp(version), Collections.emptyMap(), true), statistics));
            }

            /*
             * Get document with given version.
             */
            return getDocument(snapshot, transaction, statistics, namespace, entity, id, version, 1).thenApply(document -> PersistenceResult.readResult(document, statistics));
        });
    }

    CompletableFuture<Document> getDocument(Tuple snapshot, ReadTransaction transaction, FoundationDBStatistics statistics, String namespace, String entity, String id, Tuple version, int limit) {
        DirectorySubspace primary = getPrimary(namespace, entity);
        AsyncIterable<KeyValue> range = transaction.getRange(primary.range(Tuple.from(id, version)));
        statistics.getRange(PRIMARY_INDEX);
        AsyncIterator<KeyValue> iterator = range.iterator();
        CompletableFuture<PersistenceResult> result = new CompletableFuture<>();
        iterator.onHasNext().thenAccept(new PrimaryIterator(snapshot, statistics, namespace, entity, primary, iterator, result, limit));
        return result.thenApply(iteratorResult -> {
            if (iteratorResult.getMatches().isEmpty()) {
                return null;
            }
            return iteratorResult.getMatches().get(0);
        });
    }

    /**
     * Determine the correct first version timestamp. Perform a single database access and get range with 0 or 1 key-value.
     *
     * @param transaction
     * @param primary
     * @param id
     * @param snapshot
     * @return a completable-future that will return null if document is not found.
     */
    CompletableFuture<KeyValue> findAnyOneMatchingFragmentInPrimary(FoundationDBStatistics statistics, ReadTransaction transaction, DirectorySubspace primary, String id, Tuple snapshot) {
        /*
         * The range specified is guaranteed to never return more than 1 result. The returned KeyValue list will be one of:
         *   (1) Last fragment of matching resource when resource exists and client-timestamp is greater than or equal to resource timestamp
         *   (2) Last fragment of an unrelated resource when resource does not exist for the specified timestamp
         *   (3) KeyValue of another key-space than PRIMARY
         *   (4) Empty when database is empty (or an unlikely corner-case when asking for a resource at beginning of key-space)
         */

        Tuple tickedSnapshot = tick(snapshot);
        AsyncIterable<KeyValue> version = transaction.getRange(
                KeySelector.lastLessThan(primary.pack(Tuple.from(id, tickedSnapshot))),
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, tickedSnapshot)))
        );
        statistics.getRange(PRIMARY_INDEX);
        return version.asList().thenApply(keyValues -> {
            statistics.rangeAsList(PRIMARY_INDEX);
            if (keyValues.isEmpty()) {
                // (4) Empty
                return null;
            }
            KeyValue kv = keyValues.get(0);
            if (!primary.contains(kv.getKey())) {
                // (3) KeyValue of another key-space than PRIMARY
                return null;
            }
            Tuple keyTuple = primary.unpack(kv.getKey());
            String resourceId = keyTuple.getString(0);
            if (!id.equals(resourceId)) {
                // (2) fragment of an unrelated resource
                return null;
            }

            // (1) Match found
            return kv;
        });
    }

    @Override
    public CompletableFuture<PersistenceResult> readVersions(ZonedDateTime snapshotFrom, ZonedDateTime snapshotTo, String namespace, String entity, String id, int limit) throws PersistenceException {
        return db.readAsync(transaction -> doReadVersions(transaction, new FoundationDBStatistics(), toTuple(snapshotFrom), toTuple(snapshotTo), namespace, entity, id, limit));
    }

    CompletableFuture<PersistenceResult> doReadVersions(ReadTransaction transaction, FoundationDBStatistics statistics, Tuple snapshotFrom, Tuple snapshotTo, String namespace, String entity, String id, int limit) {
        DirectorySubspace primary = getPrimary(namespace, entity);
        return findAnyOneMatchingFragmentInPrimary(statistics, transaction, primary, id, snapshotFrom).thenCompose(aMatchingKeyValue -> {
            if (aMatchingKeyValue == null) {
                return CompletableFuture.completedFuture(PersistenceResult.readResult(Collections.emptyList(), false, statistics)); // no documents found
            }
            if (!primary.contains(aMatchingKeyValue.getKey())) {
                return CompletableFuture.completedFuture(PersistenceResult.readResult(Collections.emptyList(), false, statistics));
            }
            Tuple firstMatchingVersion = primary.unpack(aMatchingKeyValue.getKey()).getNestedTuple(1);

            /*
             * Get all fragments of all matching versions.
             */
            AsyncIterator<KeyValue> iterator = transaction.getRange(
                    KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, firstMatchingVersion))),
                    KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, snapshotTo)))
            ).iterator();
            statistics.getRange(PRIMARY_INDEX);

            CompletableFuture<PersistenceResult> result = new CompletableFuture<>();
            iterator.onHasNext().thenAccept(new PrimaryIterator(null, statistics, namespace, entity, primary, iterator, result, limit));
            return result;
        });
    }

    @Override
    public CompletableFuture<PersistenceResult> readAllVersions(String namespace, String entity, String id, int limit) throws PersistenceException {
        return db.readAsync(transaction -> doReadAllVersions(transaction, new FoundationDBStatistics(), namespace, entity, id, limit));
    }

    CompletableFuture<PersistenceResult> doReadAllVersions(ReadTransaction transaction, FoundationDBStatistics statistics, String namespace, String entity, String id, int limit) {
        DirectorySubspace primary = getPrimary(namespace, entity);
        /*
         * Get all fragments of all versions.
         */
        AsyncIterator<KeyValue> iterator = transaction.getRange(primary.range(Tuple.from(id))).iterator();
        statistics.getRange(PRIMARY_INDEX);

        CompletableFuture<PersistenceResult> result = new CompletableFuture<>();
        iterator.onHasNext().thenAccept(new PrimaryIterator(null, statistics, namespace, entity, primary, iterator, result, limit));
        return result;
    }

    @Override
    public CompletableFuture<PersistenceResult> delete(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        return db.runAsync(transaction -> doDelete(transaction, new FoundationDBStatistics(), timestamp, namespace, entity, id, policy));
    }

    private CompletableFuture<PersistenceResult> doDelete(Transaction transaction, FoundationDBStatistics statistics, ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) {
        DirectorySubspace primary = getPrimary(namespace, entity);

        Tuple timestampTuple = toTuple(timestamp);

        Document document = getDocument(timestampTuple, transaction, statistics, namespace, entity, id, timestampTuple, 1).join();

        if (document == null) {
            return CompletableFuture.completedFuture(PersistenceResult.writeResult(statistics));
        }

        // Clear primary of existing document with same version
        transaction.clear(primary.range(Tuple.from(id, timestampTuple)));
        statistics.clearRange(PRIMARY_INDEX);

        for (Fragment fragment : document.getFragmentByPath().values()) {
            String truncatedValue = truncateToMaxKeyLength(fragment.getValue());
            Tuple valueIndexKey = Tuple.from(
                    truncatedValue,
                    document.getId(),
                    timestampTuple,
                    Tuple.from(fragment.getArrayIndices())
            );
            DirectorySubspace index = getIndex(document.getNamespace(), document.getEntity(), fragment.getArrayIndicesUnawarePath());
            byte[] binaryValueIndexKey = index.pack(valueIndexKey);
            transaction.clear(binaryValueIndexKey);
            statistics.clearKeyValue(PATH_VALUE_INDEX);
        }

        return CompletableFuture.completedFuture(PersistenceResult.writeResult(statistics));
    }

    @Override
    public CompletableFuture<PersistenceResult> deleteAllVersions(String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        return db.runAsync(transaction -> doDeleteAllVersions(transaction, new FoundationDBStatistics(), namespace, entity, id, policy));
    }

    private CompletableFuture<PersistenceResult> doDeleteAllVersions(Transaction transaction, FoundationDBStatistics statistics, String namespace, String entity, String id, PersistenceDeletePolicy policy) {
        DirectorySubspace primary = getPrimary(namespace, entity);

        /*
         * Get all fragments of all versions.
         */
        Range range = primary.range(Tuple.from(id));
        AsyncIterator<KeyValue> iterator = transaction.getRange(range).iterator();
        statistics.getRange(PRIMARY_INDEX);

        CompletableFuture<Void> result = new CompletableFuture<>();

        iterator.onHasNext().thenAccept(doDeleteAllVersionsOnFragment(result, transaction, statistics, namespace, entity, id, primary, iterator));

        return result.thenApply(v -> {
            transaction.clear(range);
            return PersistenceResult.writeResult(statistics);
        });
    }

    private Consumer<Boolean> doDeleteAllVersionsOnFragment(CompletableFuture<Void> result, Transaction transaction, FoundationDBStatistics statistics, String namespace, String entity, String id, DirectorySubspace primary, AsyncIterator<KeyValue> iterator) {
        return hasNext -> {
            try {
                if (!hasNext) {
                    result.complete(null);
                    return;
                }
                KeyValue kv = iterator.next();
                Tuple key = primary.unpack(kv.getKey());
                String path = key.getString(2);
                Fragment fragment = new Fragment(path, new String(kv.getValue(), StandardCharsets.UTF_8));
                String truncatedValue = truncateToMaxKeyLength(fragment.getValue());
                Tuple version = key.getNestedTuple(1);
                Tuple arrayIndices = Tuple.from(fragment.getArrayIndices());
                Tuple valueIndexKey = Tuple.from(
                        truncatedValue,
                        id,
                        version,
                        arrayIndices
                );
                DirectorySubspace index = getIndex(namespace, entity, fragment.getArrayIndicesUnawarePath());
                byte[] binaryValueIndexKey = index.pack(valueIndexKey);
                transaction.clear(binaryValueIndexKey);
                statistics.clearKeyValue(PATH_VALUE_INDEX);
                iterator.onHasNext().thenAccept(doDeleteAllVersionsOnFragment(result, transaction, statistics, namespace, entity, id, primary, iterator));
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        };
    }


    @Override
    public CompletableFuture<PersistenceResult> markDeleted(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        return db.runAsync(transaction -> doMarkDeleted(transaction, new FoundationDBStatistics(), timestamp, namespace, entity, id, policy));
    }

    CompletableFuture<PersistenceResult> doMarkDeleted(Transaction transaction, FoundationDBStatistics statistics, ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) {
        DirectorySubspace primary = getPrimary(namespace, entity);

        Tuple timestampTuple = toTuple(timestamp);

        // Clear primary of existing document with same version
        transaction.clear(primary.range(Tuple.from(id, timestampTuple)));
        statistics.clearRange(PRIMARY_INDEX);

        /*
         * PRIMARY
         */
        Tuple primaryKey = Tuple.from(
                id,
                timestampTuple,
                DELETED_MARKER,
                0
        );
        byte[] binaryPrimaryKey = primary.pack(primaryKey);
        transaction.set(binaryPrimaryKey, EMPTY_BYTE_ARRAY);
        statistics.setKeyValue(PRIMARY_INDEX);

        return CompletableFuture.completedFuture(PersistenceResult.writeResult(statistics));
    }

    @Override
    public CompletableFuture<PersistenceResult> findAll(ZonedDateTime timestamp, String namespace, String entity, int limit) throws PersistenceException {
        return db.readAsync(transaction -> doFindAll(transaction, new FoundationDBStatistics(), toTuple(timestamp), namespace, entity, limit));
    }

    CompletableFuture<PersistenceResult> doFindAll(ReadTransaction transaction, FoundationDBStatistics statistics, Tuple snapshot, String namespace, String entity, int limit) {
        DirectorySubspace primary = getPrimary(namespace, entity);
        /*
         * Get all fragments of all versions.
         */
        AsyncIterable<KeyValue> range = transaction.getRange(primary.range(Tuple.from()));
        statistics.getRange(PRIMARY_INDEX);

        AsyncIterator<KeyValue> iterator = range.iterator();
        CompletableFuture<PersistenceResult> result = new CompletableFuture<>();
        iterator.onHasNext().thenAccept(new PrimaryIterator(snapshot, statistics, namespace, entity, primary, iterator, result, limit));

        return result;
    }

    @Override
    public CompletableFuture<PersistenceResult> find(ZonedDateTime snapshot, String namespace, String entity, String path, String value, int limit) throws PersistenceException {
        return db.readAsync(transaction -> doFind(transaction, new FoundationDBStatistics(), toTuple(snapshot), namespace, entity, path, value, limit));
    }

    CompletableFuture<PersistenceResult> doFind(ReadTransaction transaction, FoundationDBStatistics statistics, Tuple snapshot, String namespace, String entity, String path, String value, int limit) {
        String arrayIndexUnawarePath = Fragment.computeIndexUnawarePath(path, new ArrayList<>());
        DirectorySubspace primary = getPrimary(namespace, entity);
        DirectorySubspace index = getIndex(namespace, entity, arrayIndexUnawarePath);
        String truncatedValue = truncateToMaxKeyLength(value);
        AsyncIterable<KeyValue> range = transaction.getRange(index.range(Tuple.from(truncatedValue)));
        statistics.getRange(PATH_VALUE_INDEX);
        AsyncIterator<KeyValue> rangeIterator = range.iterator();

        CompletableFuture<PersistenceResult> result = new CompletableFuture<>();
        rangeIterator.onHasNext().thenAccept(new PathValueIndexIterator(this, snapshot, transaction, result, statistics, rangeIterator, primary, index, new TreeSet<>(), namespace, entity, path, value, limit));
        return result;
    }

    @Override
    public void close() throws PersistenceException {
        db.close();
    }

    static Tuple toTuple(ZonedDateTime timestamp) {
        return Tuple.from(
                timestamp.getYear(),
                timestamp.getMonth().getValue(),
                timestamp.getDayOfMonth(),
                timestamp.getHour(),
                timestamp.getMinute(),
                timestamp.getSecond(),
                TimeUnit.NANOSECONDS.toMillis(timestamp.getNano())
        );
    }

    static ZonedDateTime toTimestamp(Tuple timestampTuple) {
        return ZonedDateTime.of(
                (int) timestampTuple.getLong(0),
                (int) timestampTuple.getLong(1),
                (int) timestampTuple.getLong(2),
                (int) timestampTuple.getLong(3),
                (int) timestampTuple.getLong(4),
                (int) timestampTuple.getLong(5),
                (int) TimeUnit.MILLISECONDS.toNanos(timestampTuple.getLong(6)),
                ZONE_ID_UTC
        );
    }

    static Tuple tick(Tuple documentTimestampTuple) {
        return documentTimestampTuple.popBack().add(documentTimestampTuple.getLong(6) + 1);
    }

    static void throwRuntimeExceptionIfError(CoderResult coderResult) {
        if (coderResult.isError()) {
            try {
                coderResult.throwException();
            } catch (CharacterCodingException e) {
                throw new RuntimeException(e);
            }
            throw new IllegalStateException("Exception not thrown from coderResult.throwException()");
        }
    }
}
