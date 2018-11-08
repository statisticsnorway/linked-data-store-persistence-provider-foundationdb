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

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class FoundationDBPersistence implements Persistence {

    static final String DELETED_MARKER = "DELETED";
    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    static final ZoneId ZONE_ID_UTC = ZoneId.of("Etc/UTC");
    static final int MAX_KEY_LENGTH = 10000;
    static final int MAX_VALUE_LENGTH = 100000;

    static final String PRIMARY_INDEX = "Primary";
    static final String PATH_VALUE_INDEX = "PathValueIndex";

    final Database db;
    final Directory directory;
    final Map<Tuple, DirectorySubspace> directorySubspaceByDomainByNamespace = new ConcurrentHashMap<>();

    public FoundationDBPersistence(Database db, Directory directory) {
        this.db = db;
        this.directory = directory;
    }

    /**
     * Directory: (NAMESPACE, "Primary", ENTITY)
     * Primary:   (ID, (TIMESTAMP), PATH)  =  VALUE
     *
     * @param namespace
     * @param entity
     * @return
     */
    DirectorySubspace getPrimary(String namespace, String entity) {
        return createOrOpenDirectorySubspace(Tuple.from(namespace, "Primary", entity));
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
        return createOrOpenDirectorySubspace(Tuple.from(namespace, "PathIndex", entity, path));
    }

    DirectorySubspace createOrOpenDirectorySubspace(Tuple key) {
        // To create a nested subdirectory per tuple item, use: directory.createOrOpen(db, t.stream().map(o -> (String) o).collect(Collectors.toList())
        return directorySubspaceByDomainByNamespace.computeIfAbsent(key, t -> directory.createOrOpen(db, List.of(t.toString())).join());
    }

    @Override
    public CompletableFuture<PersistenceResult> createOrOverwrite(Document document) throws PersistenceException {
        return db.runAsync(transaction -> doCreateOrOverwrite(transaction, new FoundationDBStatistics(), document));
    }

    CompletableFuture<PersistenceResult> doCreateOrOverwrite(Transaction transaction, FoundationDBStatistics statistics, Document document) {
        DirectorySubspace primary = getPrimary(document.getNamespace(), document.getEntity());

        Tuple timestampTuple = toTuple(document.getTimestamp());

        // Clear primary of existing document with same version
        transaction.clearRangeStartsWith(primary.pack(Tuple.from(document.getId(), timestampTuple)));
        statistics.clearRangeStartsWith(PRIMARY_INDEX);

        // NOTE: With current implementation we do not need to clear the index. False-positive matches in the index
        // are always followed up by a primary lookup. Clearing Index space is expensive as it requires a read to
        // figure out whether there is anything to clear and then another read to get existing doument and then finally
        // clearing each document fragment independently from the existing document in the index space which cannot be
        // done with a single range operation and therefore must be done using individual write operations per fragment.


        for (Fragment fragment : document.getFragments()) {

            /*
             * PRIMARY
             */
            Tuple primaryKey = Tuple.from(
                    document.getId(),
                    timestampTuple,
                    fragment.getPath()
            );
            byte[] binaryPrimaryKey = primary.pack(primaryKey);
            byte[] binaryPrimaryValue = fragment.getValue().getBytes(StandardCharsets.UTF_8);
            if (binaryPrimaryKey.length > MAX_KEY_LENGTH) {
                throw new IllegalArgumentException("Document fragment key is too big for primary, at most " + MAX_KEY_LENGTH + " bytes allowed. Was: " + binaryPrimaryKey.length + " bytes.");
            }
            if (binaryPrimaryValue.length > MAX_VALUE_LENGTH) {
                throw new IllegalArgumentException("Document fragment value is too big for primary, at most " + MAX_VALUE_LENGTH + " bytes allowed. Was: " + binaryPrimaryValue.length + " bytes.");
            }
            transaction.set(binaryPrimaryKey, binaryPrimaryValue);
            statistics.setKeyValue(PRIMARY_INDEX);

            /*
             * INDEX
             */
            String truncatedValue = truncateToMaxKeyLength(fragment.getValue());
            Tuple valueIndexKey = Tuple.from(
                    truncatedValue,
                    document.getId(),
                    timestampTuple,
                    Tuple.from(fragment.getArrayIndices())
            );
            DirectorySubspace index = getIndex(document.getNamespace(), document.getEntity(), fragment.getArrayIndicesUnawarePath());
            byte[] binaryValueIndexKey = index.pack(valueIndexKey);
            byte[] binaryValueIndexValue = (truncatedValue.length() == fragment.getValue().length()) ? EMPTY_BYTE_ARRAY : binaryPrimaryValue;
            if (binaryValueIndexKey.length > MAX_KEY_LENGTH) {
                throw new IllegalArgumentException("Document fragment key is too big for index, at most " + MAX_KEY_LENGTH + " bytes allowed. Was: " + binaryValueIndexKey.length + " bytes.");
            }
            transaction.set(binaryValueIndexKey, binaryValueIndexValue);
            statistics.setKeyValue(PATH_VALUE_INDEX);

        }

        return CompletableFuture.completedFuture(PersistenceResult.writeResult(statistics));
    }

    static String truncateToMaxKeyLength(String input) {
        return input.substring(0, Math.min(input.length(), MAX_KEY_LENGTH - 200));
    }

    @Override
    public CompletableFuture<PersistenceResult> read(ZonedDateTime timestamp, String namespace, String entity, String id) throws PersistenceException {
        return db.readAsync(transaction -> doRead(transaction, new FoundationDBStatistics(), timestamp, namespace, entity, id));
    }

    CompletableFuture<PersistenceResult> doRead(ReadTransaction transaction, FoundationDBStatistics statistics, ZonedDateTime timestamp, String namespace, String entity, String id) {
        DirectorySubspace primary = getPrimary(namespace, entity);

        /*
         * Determine the correct version timestamp of the document to use. Will perform a database access and fetch at most one key-value
         */
        return findAnyOneMatchingFragmentInPrimary(statistics, transaction, primary, id, toTuple(timestamp)).thenCompose(aMatchingKeyValue -> {
            if (aMatchingKeyValue == null) {
                // document not found
                return CompletableFuture.completedFuture(PersistenceResult.readResult(null, statistics));
            }
            Tuple version = primary.unpack(aMatchingKeyValue.getKey()).getNestedTuple(1);
            if (DELETED_MARKER.equals(primary.unpack(aMatchingKeyValue.getKey()).getString(2))) {
                return CompletableFuture.completedFuture(PersistenceResult.readResult(new Document(namespace, entity, id, toTimestamp(version), Collections.emptyNavigableSet(), true), statistics));
            }

            /*
             * Get document with given version.
             */
            return getDocument(transaction, statistics, namespace, entity, id, version).thenApply(document -> PersistenceResult.readResult(document, statistics));
        });
    }

    private CompletableFuture<Document> getDocument(ReadTransaction transaction, FoundationDBStatistics statistics, String namespace, String entity, String id, Tuple version) {
        DirectorySubspace primary = getPrimary(namespace, entity);
        Tuple documentTimestampTuplePlusOneTick = tick(version);
        AsyncIterable<KeyValue> range = transaction.getRange(
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, version))),
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, documentTimestampTuplePlusOneTick)))
        );
        statistics.getRange(PRIMARY_INDEX);
        AsyncIterator<KeyValue> iterator = range.iterator();
        final NavigableSet<Fragment> fragments = Collections.synchronizedNavigableSet(new TreeSet<>());
        final CompletableFuture<Document> documentCompletableFuture = new CompletableFuture<>();
        iterator.onHasNext().thenAccept(deleteAllVersionsOnFragment(statistics, iterator, documentCompletableFuture, fragments, primary, namespace, entity, id, version));
        return documentCompletableFuture;
    }

    private Consumer<Boolean> deleteAllVersionsOnFragment(FoundationDBStatistics statistics, AsyncIterator<KeyValue> iterator, CompletableFuture<Document> documentCompletableFuture, NavigableSet<Fragment> fragments, DirectorySubspace primary, String namespace, String entity, String id, Tuple version) {
        return hasNext -> {
            try {
                if (hasNext) {
                    KeyValue kv = iterator.next();
                    statistics.rangeIteratorNext(PRIMARY_INDEX);
                    Tuple keyTuple = primary.unpack(kv.getKey());
                    String value = new String(kv.getValue(), StandardCharsets.UTF_8);
                    String path = keyTuple.getString(2);
                    fragments.add(new Fragment(path, value));
                    iterator.onHasNext().thenAccept(deleteAllVersionsOnFragment(statistics, iterator, documentCompletableFuture, fragments, primary, namespace, entity, id, version));
                } else {
                    Document document = null;
                    if (!fragments.isEmpty()) {
                        if (DELETED_MARKER.equals(fragments.first().getPath())) {
                            document = new Document(namespace, entity, id, toTimestamp(version), Collections.emptyNavigableSet(), true);
                        } else {
                            document = new Document(namespace, entity, id, toTimestamp(version), fragments, false);
                        }
                    }
                    documentCompletableFuture.complete(document);
                }
            } catch (Throwable t) {
                documentCompletableFuture.completeExceptionally(t);
            }
        };
    }

    /**
     * Determine the correct first version timestamp. Perform a single database access and get range with 0 or 1 key-value.
     *
     * @param transaction
     * @param primary
     * @param id
     * @param timestamp
     * @return a completable-future that will return null if document is not found.
     */
    CompletableFuture<KeyValue> findAnyOneMatchingFragmentInPrimary(FoundationDBStatistics statistics, ReadTransaction transaction, DirectorySubspace primary, String id, Tuple timestamp) {
        /*
         * The range specified is guaranteed to never return more than 1 result. The returned KeyValue list will be one of:
         *   (1) Last fragment of matching resource when resource exists and client-timestamp is greater than or equal to resource timestamp
         *   (2) Last fragment of an unrelated resource when resource does not exist for the specified timestamp
         *   (3) KeyValue of another key-space than PRIMARY
         *   (4) Empty when database is empty (or an unlikely corner-case when asking for a resource at beginning of key-space)
         */

        Tuple tickedTimestampTuple = tick(timestamp);
        AsyncIterable<KeyValue> version = transaction.getRange(
                KeySelector.lastLessThan(primary.pack(Tuple.from(id, tickedTimestampTuple))),
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, tickedTimestampTuple)))
        );
        statistics.getRange(PRIMARY_INDEX);
        return version.asList().thenApply(keyValues -> {
            statistics.rangeAsList(PRIMARY_INDEX);
            if (keyValues.isEmpty()) {
                // (4) Empty
                return null;
            }
            KeyValue kv = keyValues.get(0);
            try {
                // TODO check whether we are still in key-space explicitly before parsing key according to PRIMARY
                // TODO layout. This will improve case (3) to cover all corner-cases.
                Tuple keyTuple = primary.unpack(kv.getKey());
                String resourceId = keyTuple.getString(0);
                if (!id.equals(resourceId)) {
                    // (2) fragment of an unrelated resource
                    return null;
                }
            } catch (RuntimeException e) {
                // (3) KeyValue of another key-space than PRIMARY
                return null;
            }

            // (1) Match found
            return kv;
        });
    }

    @Override
    public CompletableFuture<PersistenceResult> readVersions(ZonedDateTime from, ZonedDateTime to, String namespace, String entity, String id, int limit) throws PersistenceException {
        return db.readAsync(transaction -> doReadVersions(transaction, new FoundationDBStatistics(), from, to, namespace, entity, id, limit));
    }

    CompletableFuture<PersistenceResult> doReadVersions(ReadTransaction transaction, FoundationDBStatistics statistics, ZonedDateTime from, ZonedDateTime to, String namespace, String entity, String id, int limit) {
        DirectorySubspace primary = getPrimary(namespace, entity);
        return findAnyOneMatchingFragmentInPrimary(statistics, transaction, primary, id, toTuple(from)).thenApply(aMatchingKeyValue -> {
            if (aMatchingKeyValue == null) {
                return PersistenceResult.readResult(Collections.emptyList(), false, statistics); // no documents found
            }
            Tuple firstMatchingVersion = primary.unpack(aMatchingKeyValue.getKey()).getNestedTuple(1);

            /*
             * Get all fragments of all matching versions.
             */
            AsyncIterable<KeyValue> range = transaction.getRange(
                    KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, firstMatchingVersion))),
                    KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, toTuple(to))))
            );
            statistics.getRange(PRIMARY_INDEX);

            CompletableFuture<PersistenceResult> documents = getDocuments(statistics, namespace, entity, range, limit);
            return documents.join();
        });
    }

    private CompletableFuture<PersistenceResult> getDocuments(FoundationDBStatistics statistics, String namespace, String entity, AsyncIterable<KeyValue> range, int limit) {
        final DirectorySubspace primary = getPrimary(namespace, entity);
        final List<Document> documents = new ArrayList<>();
        final List<Fragment> fragments = new ArrayList<>();
        final AsyncIterator<KeyValue> iterator = range.iterator();
        final CompletableFuture<PersistenceResult> documentsCompletableFuture = new CompletableFuture<>();
        final AtomicReference<Tuple> prevVersionRef = new AtomicReference<>();
        iterator.onHasNext().thenAccept(onNextFragment(statistics, namespace, entity, null, primary, documents, fragments, iterator, documentsCompletableFuture, prevVersionRef, limit));
        return documentsCompletableFuture;
    }

    private Consumer<Boolean> onNextFragment(FoundationDBStatistics statistics, String namespace, String entity, String id, DirectorySubspace primary, List<Document> documents, List<Fragment> fragments, AsyncIterator<KeyValue> iterator, CompletableFuture<PersistenceResult> documentsCompletableFuture, AtomicReference<Tuple> prevVersionRef, int limit) {
        return hasNext -> {
            try {
                if (hasNext) {
                    KeyValue kv = iterator.next();
                    statistics.rangeIteratorNext(PRIMARY_INDEX);
                    Tuple keyTuple = primary.unpack(kv.getKey());
                    String value = new String(kv.getValue(), StandardCharsets.UTF_8);
                    String dbId = keyTuple.getString(0);
                    String path = keyTuple.getString(2);
                    Tuple timestampTuple = keyTuple.getNestedTuple(1);
                    if (id != null) {
                        if (!id.equals(dbId) || prevVersionRef.get() != null && !timestampTuple.equals(prevVersionRef.get())) {
                            if (documents.size() >= limit) {
                                documentsCompletableFuture.complete(PersistenceResult.readResult(documents, true, statistics));
                                iterator.cancel();
                                statistics.rangeIteratorCancel(PRIMARY_INDEX);
                            }
                            Document document;
                            if (DELETED_MARKER.equals(fragments.get(0).getPath())) {
                                document = new Document(namespace, entity, id, toTimestamp(prevVersionRef.get()), Collections.emptyNavigableSet(), true);
                            } else {
                                document = new Document(namespace, entity, id, toTimestamp(prevVersionRef.get()), new TreeSet<>(fragments), false);
                            }
                            documents.add(document);
                            fragments.clear();
                        }
                    }
                    fragments.add(new Fragment(path, value));
                    prevVersionRef.set(keyTuple.getNestedTuple(1));
                    if (!documentsCompletableFuture.isDone()) {
                        iterator.onHasNext().thenAccept(onNextFragment(statistics, namespace, entity, dbId, primary, documents, fragments, iterator, documentsCompletableFuture, prevVersionRef, limit));
                    }
                } else {
                    if (id != null) {
                        Document document;
                        if (DELETED_MARKER.equals(fragments.get(0).getPath())) {
                            document = new Document(namespace, entity, id, toTimestamp(prevVersionRef.get()), Collections.emptyNavigableSet(), true);
                        } else {
                            document = new Document(namespace, entity, id, toTimestamp(prevVersionRef.get()), new TreeSet<>(fragments), false);
                        }
                        documents.add(document);
                    }
                    documentsCompletableFuture.complete(PersistenceResult.readResult(documents, false, statistics));
                }
            } catch (Throwable t) {
                documentsCompletableFuture.completeExceptionally(t);
            }
        }

                ;
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
        AsyncIterable<KeyValue> range = transaction.getRange(
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id))),
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id + " ")))
        );
        statistics.getRange(PRIMARY_INDEX);

        CompletableFuture<PersistenceResult> result = getDocuments(statistics, namespace, entity, range, limit);
        return result;
    }

    @Override
    public CompletableFuture<PersistenceResult> delete(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        return db.runAsync(transaction -> doDelete(transaction, new FoundationDBStatistics(), timestamp, namespace, entity, id, policy));
    }

    private CompletableFuture<PersistenceResult> doDelete(Transaction transaction, FoundationDBStatistics statistics, ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) {
        DirectorySubspace primary = getPrimary(namespace, entity);

        Tuple timestampTuple = toTuple(timestamp);

        Document document = getDocument(transaction, statistics, namespace, entity, id, timestampTuple).join();

        if (document == null) {
            return CompletableFuture.completedFuture(PersistenceResult.writeResult(statistics));
        }

        // Clear primary of existing document with same version
        transaction.clearRangeStartsWith(primary.pack(Tuple.from(id, timestampTuple)));
        statistics.clearRangeStartsWith(PRIMARY_INDEX);

        for (Fragment fragment : document.getFragments()) {
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
        return db.runAsync(transaction -> deleteAllVersions(transaction, new FoundationDBStatistics(), namespace, entity, id, policy));
    }

    private CompletableFuture<PersistenceResult> deleteAllVersions(Transaction transaction, FoundationDBStatistics statistics, String namespace, String entity, String id, PersistenceDeletePolicy policy) {
        DirectorySubspace primary = getPrimary(namespace, entity);

        /*
         * Get all fragments of all versions.
         */
        Range range = primary.range(Tuple.from(id));
        AsyncIterator<KeyValue> iterator = transaction.getRange(range).iterator();
        statistics.getRange(PRIMARY_INDEX);

        CompletableFuture<Void> result = new CompletableFuture<>();

        iterator.onHasNext().thenAccept(deleteAllVersionsOnFragment(result, transaction, statistics, namespace, entity, id, primary, iterator));

        return result.thenApply(v -> {
            transaction.clear(range);
            return PersistenceResult.writeResult(statistics);
        });
    }

    private Consumer<Boolean> deleteAllVersionsOnFragment(CompletableFuture<Void> result, Transaction transaction, FoundationDBStatistics statistics, String namespace, String entity, String id, DirectorySubspace primary, AsyncIterator<KeyValue> iterator) {
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
                iterator.onHasNext().thenAccept(deleteAllVersionsOnFragment(result, transaction, statistics, namespace, entity, id, primary, iterator));
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
        transaction.clearRangeStartsWith(primary.pack(Tuple.from(id, timestampTuple)));
        statistics.clearRangeStartsWith(PRIMARY_INDEX);

        /*
         * PRIMARY
         */
        Tuple primaryKey = Tuple.from(
                id,
                timestampTuple,
                DELETED_MARKER
        );
        byte[] binaryPrimaryKey = primary.pack(primaryKey);
        transaction.set(binaryPrimaryKey, EMPTY_BYTE_ARRAY);
        statistics.setKeyValue(PRIMARY_INDEX);

        return CompletableFuture.completedFuture(PersistenceResult.writeResult());
    }

    @Override
    public CompletableFuture<PersistenceResult> findAll(ZonedDateTime timestamp, String namespace, String entity, int limit) throws PersistenceException {
        return db.readAsync(transaction -> doFindAll(transaction, new FoundationDBStatistics(), timestamp, namespace, entity, limit));
    }

    CompletableFuture<PersistenceResult> doFindAll(ReadTransaction transaction, FoundationDBStatistics statistics, ZonedDateTime timestamp, String namespace, String entity, int limit) {
        final DirectorySubspace primary = getPrimary(namespace, entity);
        /*
         * Get all fragments of all versions.
         */
        final AsyncIterable<KeyValue> range = transaction.getRange(
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from())),
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from("~")))
        );
        statistics.getRange(PRIMARY_INDEX);

        final Map<String, Document> documentById = new ConcurrentHashMap<>();
        final List<Fragment> fragments = new ArrayList<>();
        final AsyncIterator<KeyValue> iterator = range.iterator();
        CompletableFuture<PersistenceResult> documentsCompletableFuture = new CompletableFuture<>();
        iterator.onHasNext().thenAccept(onNextFragment(statistics, toTuple(timestamp), namespace, entity, null, primary, documentById, fragments, iterator, documentsCompletableFuture, null, limit));

        return documentsCompletableFuture;
    }

    private Consumer<Boolean> onNextFragment(FoundationDBStatistics statistics, Tuple timestampTuple, String namespace, String entity, String id, DirectorySubspace primary, Map<String, Document> documentById, List<Fragment> fragments, AsyncIterator<KeyValue> iterator, CompletableFuture<PersistenceResult> documentsCompletableFuture, Tuple prevVersion, int limit) {
        return hasNext -> {
            try {
                if (hasNext) {
                    KeyValue kv = iterator.next();
                    statistics.rangeIteratorNext(PRIMARY_INDEX);
                    Tuple keyTuple = primary.unpack(kv.getKey());
                    String value = new String(kv.getValue(), StandardCharsets.UTF_8);
                    String dbId = keyTuple.getString(0);
                    String path = keyTuple.getString(2);
                    Tuple versionTuple = keyTuple.getNestedTuple(1);
                    if (id != null) {
                        if (!id.equals(dbId) || (prevVersion != null && !versionTuple.equals(prevVersion))) {
                            if (!id.equals(dbId) || prevVersion.compareTo(timestampTuple) <= 0) {
                                Document document;
                                if (DELETED_MARKER.equals(fragments.get(0).getPath())) {
                                    document = new Document(namespace, entity, id, toTimestamp(prevVersion), Collections.emptyNavigableSet(), true);
                                } else {
                                    document = new Document(namespace, entity, id, toTimestamp(prevVersion), new TreeSet<>(fragments), false);
                                }
                                documentById.put(id, document); // overwrite any earlier versions
                            }
                            fragments.clear();
                        }
                    }
                    fragments.add(new Fragment(path, value));
                    if (documentById.size() < limit) {
                        iterator.onHasNext().thenAccept(onNextFragment(statistics, timestampTuple, namespace, entity, dbId, primary, documentById, fragments, iterator, documentsCompletableFuture, versionTuple, limit));
                    } else {
                        // limit reached
                        documentsCompletableFuture.complete(PersistenceResult.readResult(new ArrayList<>(documentById.values()), true));
                        iterator.cancel();
                        statistics.rangeIteratorCancel(PRIMARY_INDEX);
                    }
                } else {
                    if (id != null && prevVersion.compareTo(timestampTuple) <= 0) {
                        Document document;
                        if (DELETED_MARKER.equals(fragments.get(0).getPath())) {
                            document = new Document(namespace, entity, id, toTimestamp(prevVersion), Collections.emptyNavigableSet(), true);
                        } else {
                            document = new Document(namespace, entity, id, toTimestamp(prevVersion), new TreeSet<>(fragments), false);
                        }
                        documentById.put(id, document);
                    }
                    documentsCompletableFuture.complete(PersistenceResult.readResult(new ArrayList<>(documentById.values()), false));
                }
            } catch (Throwable t) {
                documentsCompletableFuture.completeExceptionally(t);
            }
        };
    }

    @Override
    public CompletableFuture<PersistenceResult> find(ZonedDateTime timestamp, String namespace, String entity, String path, String value, int limit) throws PersistenceException {
        return db.readAsync(transaction -> doFind(transaction, new FoundationDBStatistics(), timestamp, namespace, entity, path, value, limit));
    }

    CompletableFuture<PersistenceResult> doFind(ReadTransaction transaction, FoundationDBStatistics statistics, ZonedDateTime timestamp, String namespace, String entity, String path, String value, int limit) {
        String arrayIndexUnawarePath = path.replaceAll(Fragment.arrayIndexPattern.pattern(), "[]");
        DirectorySubspace primary = getPrimary(namespace, entity);
        DirectorySubspace index = getIndex(namespace, entity, arrayIndexUnawarePath);
        Tuple timestampTuple = toTuple(timestamp);
        String truncatedValue = truncateToMaxKeyLength(value);
        AsyncIterable<KeyValue> range = transaction.getRange(
                KeySelector.firstGreaterOrEqual(index.pack(Tuple.from(truncatedValue))),
                KeySelector.firstGreaterOrEqual(index.pack(Tuple.from(truncatedValue + " ")))
        );
        statistics.getRange(PATH_VALUE_INDEX);
        AsyncIterator<KeyValue> rangeIterator = range.iterator();

        CompletableFuture<PersistenceResult> result = new CompletableFuture<>();
        rangeIterator.onHasNext().thenAccept(new DoFindOnNextIterator(timestampTuple, transaction, result, statistics, rangeIterator, primary, index, new TreeSet<>(), namespace, entity, value, limit));
        return result;
    }

    class DoFindOnNextIterator implements Consumer<Boolean> {
        final Tuple timestamp;
        final ReadTransaction transaction;
        final CompletableFuture<PersistenceResult> result;
        final FoundationDBStatistics statistics;
        final AsyncIterator<KeyValue> rangeIterator;
        final DirectorySubspace primary;
        final DirectorySubspace index;
        final NavigableSet<Tuple> versions;
        final String namespace;
        final String entity;
        final String value;
        final int limit;
        final List<Document> documents = Collections.synchronizedList(new ArrayList<>());
        final List<CompletableFuture<Document>> primaryLookupFutures = Collections.synchronizedList(new ArrayList<>());

        final AtomicInteger indexMatches = new AtomicInteger(0);
        final AtomicReference<String> versionsId = new AtomicReference<>();

        public DoFindOnNextIterator(Tuple timestamp, ReadTransaction transaction, CompletableFuture<PersistenceResult> result, FoundationDBStatistics statistics, AsyncIterator<KeyValue> rangeIterator, DirectorySubspace primary, DirectorySubspace index, NavigableSet<Tuple> versions, String namespace, String entity, String value, int limit) {
            this.transaction = transaction;
            this.timestamp = timestamp;
            this.result = result;
            this.statistics = statistics;
            this.rangeIterator = rangeIterator;
            this.primary = primary;
            this.index = index;
            this.versions = versions;
            this.namespace = namespace;
            this.entity = entity;
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
                    if (version.compareTo(timestamp) <= 0) {
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
            return findAnyOneMatchingFragmentInPrimary(statistics, transaction, primary, id, timestamp).thenCompose(aMatchingFragmentKv -> {
                Tuple keyTuple = primary.unpack(aMatchingFragmentKv.getKey());
                Tuple versionTuple = keyTuple.getNestedTuple(1);
                if (!version.equals(versionTuple)) {
                    return null; // false-positive index-match on older version
                }
                if (DELETED_MARKER.equals(keyTuple.getString(2))) {
                    // Version was overwritten in primary by a delete-marker, schedule task to remove index fragment.
                    db.runAsync(trn -> doClearKeyValue(trn, aMatchingFragmentKv)).exceptionally(throwable -> {
                        throwable.printStackTrace();
                        return null;
                    });
                    return CompletableFuture.completedFuture(new Document(namespace, entity, id, toTimestamp(versionTuple), Collections.emptyNavigableSet(), true));
                }
                return getDocument(transaction, statistics, namespace, entity, id, versionTuple).thenApply(doc -> {
                    if (doc == null) {
                        db.runAsync(trn -> doClearKeyValue(trn, aMatchingFragmentKv)).exceptionally(throwable -> {
                            throwable.printStackTrace();
                            return null;
                        });
                        return null;
                    }
                    documents.add(doc);
                    return doc;
                });
            });
        }

        void onHasNoMore() {
            for (Tuple version : versions.descendingSet()) {
                if (version.compareTo(timestamp) <= 0) {
                    indexMatches.incrementAndGet();
                    primaryLookupFutures.add(onIndexMatch(versionsId.get(), version));
                    break;
                }
            }
            CompletableFuture.allOf(primaryLookupFutures.toArray(CompletableFuture[]::new)).join();
            result.complete(PersistenceResult.readResult(List.copyOf(documents), false, statistics));
        }

    }

    private CompletableFuture<PersistenceResult> doClearKeyValue(Transaction trn, KeyValue aMatchingFragmentKv) {
        FoundationDBStatistics stat = new FoundationDBStatistics();
        trn.clear(aMatchingFragmentKv.getKey());
        stat.clearKeyValue(PATH_VALUE_INDEX);
        return CompletableFuture.completedFuture(PersistenceResult.writeResult(stat));
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
}
