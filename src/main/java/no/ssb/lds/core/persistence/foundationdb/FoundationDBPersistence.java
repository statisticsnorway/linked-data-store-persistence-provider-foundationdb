package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
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

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class FoundationDBPersistence implements Persistence {

    static final String DELETED_MARKER = "DELETED";
    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    static final ZoneId ZONE_ID_UTC = ZoneId.of("Etc/UTC");
    static final int MAX_KEY_LENGTH = 10000;
    static final int MAX_VALUE_LENGTH = 100000;

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
     * PathIndex: (VALUE, (TIMESTAMP), ARRAY-INDICES-FROM-PATH, ID) = EMPTY
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
    public void createOrOverwrite(Document document) throws PersistenceException {
        db.run(transaction -> doCreateOrOverwrite(transaction, document));
    }

    Object doCreateOrOverwrite(Transaction transaction, Document document) {
        DirectorySubspace primary = getPrimary(document.getNamespace(), document.getEntity());

        Tuple timestampTuple = toTuple(document.getTimestamp());


        // Clear primary of existing document with same version
        transaction.clear(primary.range(Tuple.from(document.getId(), timestampTuple)));

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

            /*
             * INDEX
             */
            String truncatedValue = truncateToMaxKeyLength(fragment.getValue());
            Tuple valueIndexKey = Tuple.from(
                    truncatedValue,
                    timestampTuple,
                    Tuple.from(fragment.getArrayIndices()),
                    document.getId()
            );
            DirectorySubspace index = getIndex(document.getNamespace(), document.getEntity(), fragment.getArrayIndicesUnawarePath());
            byte[] binaryValueIndexKey = index.pack(valueIndexKey);
            byte[] binaryValueIndexValue = (truncatedValue.length() == fragment.getValue().length()) ? EMPTY_BYTE_ARRAY : binaryPrimaryValue;
            if (binaryValueIndexKey.length > MAX_KEY_LENGTH) {
                throw new IllegalArgumentException("Document fragment key is too big for index, at most " + MAX_KEY_LENGTH + " bytes allowed. Was: " + binaryValueIndexKey.length + " bytes.");
            }
            transaction.set(binaryValueIndexKey, binaryValueIndexValue);

        }

        return null;
    }

    static String truncateToMaxKeyLength(String input) {
        return input.substring(0, Math.min(input.length(), MAX_KEY_LENGTH - 200));
    }

    @Override
    public Document read(ZonedDateTime timestamp, String namespace, String entity, String id) throws PersistenceException {
        return db.read(transaction -> doRead(transaction, timestamp, namespace, entity, id));
    }

    Document doRead(ReadTransaction transaction, ZonedDateTime timestamp, String namespace, String entity, String id) {
        DirectorySubspace directorySubspace = getPrimary(namespace, entity);

        /*
         * Determine the correct version timestamp of the document to use. Will perform a database access and fetch at most one key-value
         */
        CompletableFuture<Document> matchingDocument = findAnyOneMatchingFragmentInPrimary(transaction, directorySubspace, id, timestamp).thenApply(aMatchingKeyValue -> {
            if (aMatchingKeyValue == null) {
                // document not found
                return null;
            }
            Tuple version = directorySubspace.unpack(aMatchingKeyValue.getKey()).getNestedTuple(1);
            if (DELETED_MARKER.equals(directorySubspace.unpack(aMatchingKeyValue.getKey()).getString(2))) {
                return new Document(namespace, entity, id, toTimestamp(version), Collections.emptyList(), true);
            }

            /*
             * Get document with given version.
             */
            CompletableFuture<Document> document = getDocument(transaction, namespace, entity, id, version);
            return document.join();
        });
        return matchingDocument.join();
    }

    private CompletableFuture<Document> getDocument(ReadTransaction transaction, String namespace, String entity, String id, Tuple version) {
        DirectorySubspace primary = getPrimary(namespace, entity);
        Tuple documentTimestampTuplePlusOneTick = tick(version);
        AsyncIterable<KeyValue> range = transaction.getRange(
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, version))),
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, documentTimestampTuplePlusOneTick)))
        );
        AsyncIterator<KeyValue> iterator = range.iterator();
        final List<Fragment> fragments = Collections.synchronizedList(new ArrayList<>());
        final CompletableFuture<Document> documentCompletableFuture = new CompletableFuture<>();
        iterator.onHasNext().thenAccept(getBooleanConsumer(iterator, documentCompletableFuture, fragments, primary, namespace, entity, id, version));
        return documentCompletableFuture;
    }

    private Consumer<Boolean> getBooleanConsumer(AsyncIterator<KeyValue> iterator, CompletableFuture<Document> documentCompletableFuture, List<Fragment> fragments, DirectorySubspace primary, String namespace, String entity, String id, Tuple version) {
        return hasNext -> {
            try {
                if (hasNext) {
                    KeyValue kv = iterator.next();
                    Tuple keyTuple = primary.unpack(kv.getKey());
                    String value = new String(kv.getValue(), StandardCharsets.UTF_8);
                    String path = keyTuple.getString(2);
                    fragments.add(new Fragment(path, value));
                    iterator.onHasNext().thenAccept(getBooleanConsumer(iterator, documentCompletableFuture, fragments, primary, namespace, entity, id, version));
                } else {
                    Document document = null;
                    if (!fragments.isEmpty()) {
                        if (DELETED_MARKER.equals(fragments.get(0).getPath())) {
                            document = new Document(namespace, entity, id, toTimestamp(version), Collections.emptyList(), true);
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
    CompletableFuture<KeyValue> findAnyOneMatchingFragmentInPrimary(ReadTransaction transaction, DirectorySubspace primary, String id, ZonedDateTime timestamp) {
        /*
         * The range specified is guaranteed to never return more than 1 result. The returned KeyValue list will be one of:
         *   (1) Last fragment of matching resource when resource exists and client-timestamp is greater than or equal to resource timestamp
         *   (2) Last fragment of an unrelated resource when resource does not exist for the specified timestamp
         *   (3) KeyValue of another key-space than PRIMARY
         *   (4) Empty when database is empty (or an unlikely corner-case when asking for a resource at beginning of key-space)
         */

        Tuple tickedTimestampTuple = tick(toTuple(timestamp));
        AsyncIterable<KeyValue> version = transaction.getRange(
                KeySelector.lastLessThan(primary.pack(Tuple.from(id, tickedTimestampTuple))),
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, tickedTimestampTuple)))
        );
        return version.asList().thenApply(keyValues -> {
            if (keyValues.isEmpty()) {
                // (4) Empty
                return null;
            }
            KeyValue kv = keyValues.get(0);
            try {
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
    public List<Document> readVersions(ZonedDateTime from, ZonedDateTime to, String namespace, String entity, String id, int limit) throws PersistenceException {
        return db.read(transaction -> doReadVersions(transaction, from, to, namespace, entity, id, limit).join());
    }

    CompletableFuture<List<Document>> doReadVersions(ReadTransaction transaction, ZonedDateTime from, ZonedDateTime to, String namespace, String entity, String id, int limit) {
        // TODO use limit
        DirectorySubspace directorySubspace = getPrimary(namespace, entity);

        return findAnyOneMatchingFragmentInPrimary(transaction, directorySubspace, id, from).thenApply(aMatchingKeyValue -> {
            if (aMatchingKeyValue == null) {
                return Collections.emptyList(); // no documents found
            }
            Tuple firstMatchingVersion = directorySubspace.unpack(aMatchingKeyValue.getKey()).getNestedTuple(1);

            /*
             * Get all fragments of all matching versions.
             */
            AsyncIterable<KeyValue> range = transaction.getRange(
                    KeySelector.firstGreaterOrEqual(directorySubspace.pack(Tuple.from(id, firstMatchingVersion))),
                    KeySelector.firstGreaterOrEqual(directorySubspace.pack(Tuple.from(id, toTuple(to))))
            );

            CompletableFuture<List<Document>> documents = getDocuments(namespace, entity, range);
            return documents.join();
        });
    }

    private CompletableFuture<List<Document>> getDocuments(String namespace, String entity, AsyncIterable<KeyValue> range) {
        final DirectorySubspace primary = getPrimary(namespace, entity);
        final List<Document> documents = new ArrayList<>();
        final List<Fragment> fragments = new ArrayList<>();
        final AsyncIterator<KeyValue> iterator = range.iterator();
        final CompletableFuture<List<Document>> documentsCompletableFuture = new CompletableFuture<>();
        final AtomicReference<Tuple> prevVersionRef = new AtomicReference<>();
        iterator.onHasNext().thenAccept(onNextFragment(namespace, entity, null, primary, documents, fragments, iterator, documentsCompletableFuture, prevVersionRef));
        return documentsCompletableFuture;
    }

    private Consumer<Boolean> onNextFragment(String namespace, String entity, String id, DirectorySubspace primary, List<Document> documents, List<Fragment> fragments, AsyncIterator<KeyValue> iterator, CompletableFuture<List<Document>> documentsCompletableFuture, AtomicReference<Tuple> prevVersionRef) {
        return hasNext -> {
            try {
                if (hasNext) {
                    KeyValue kv = iterator.next();
                    Tuple keyTuple = primary.unpack(kv.getKey());
                    String value = new String(kv.getValue(), StandardCharsets.UTF_8);
                    String dbId = keyTuple.getString(0);
                    String path = keyTuple.getString(2);
                    Tuple timestampTuple = keyTuple.getNestedTuple(1);
                    if (id != null) {
                        if (!id.equals(dbId) || prevVersionRef.get() != null && !timestampTuple.equals(prevVersionRef.get())) {
                            Document document;
                            if (DELETED_MARKER.equals(fragments.get(0).getPath())) {
                                document = new Document(namespace, entity, id, toTimestamp(prevVersionRef.get()), Collections.emptyList(), true);
                            } else {
                                document = new Document(namespace, entity, id, toTimestamp(prevVersionRef.get()), List.copyOf(fragments), false);
                            }
                            documents.add(document);
                            fragments.clear();
                        }
                    }
                    fragments.add(new Fragment(path, value));
                    prevVersionRef.set(keyTuple.getNestedTuple(1));
                    iterator.onHasNext().thenAccept(onNextFragment(namespace, entity, dbId, primary, documents, fragments, iterator, documentsCompletableFuture, prevVersionRef));
                } else {
                    if (id != null) {
                        Document document;
                        if (DELETED_MARKER.equals(fragments.get(0).getPath())) {
                            document = new Document(namespace, entity, id, toTimestamp(prevVersionRef.get()), Collections.emptyList(), true);
                        } else {
                            document = new Document(namespace, entity, id, toTimestamp(prevVersionRef.get()), List.copyOf(fragments), false);
                        }
                        documents.add(document);
                    }
                    documentsCompletableFuture.complete(documents);
                }
            } catch (Throwable t) {
                documentsCompletableFuture.completeExceptionally(t);
            }
        };
    }

    @Override
    public List<Document> readAllVersions(String namespace, String entity, String id, int limit) throws PersistenceException {
        return db.read(transaction -> doReadAllVersions(transaction, namespace, entity, id, limit));
    }

    List<Document> doReadAllVersions(ReadTransaction transaction, String namespace, String entity, String id, int limit) {
        // TODO use limit
        DirectorySubspace primary = getPrimary(namespace, entity);
        /*
         * Get all fragments of all versions.
         */
        AsyncIterable<KeyValue> range = transaction.getRange(
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id))),
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id + " ")))
        );

        CompletableFuture<List<Document>> documents = getDocuments(namespace, entity, range);
        return documents.join();
    }

    @Override
    public void delete(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
    }

    @Override
    public void markDeleted(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        db.run(transaction -> doMarkDeleted(transaction, timestamp, namespace, entity, id, policy));
    }

    Object doMarkDeleted(Transaction transaction, ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) {
        DirectorySubspace primary = getPrimary(namespace, entity);

        Tuple timestampTuple = toTuple(timestamp);

        // Clear primary of existing document with same version
        transaction.clear(primary.range(Tuple.from(id, timestampTuple)));

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

        return null;
    }

    @Override
    public List<Document> findAll(ZonedDateTime timestamp, String namespace, String entity, int limit) throws PersistenceException {
        return db.read(transaction -> doFindAll(transaction, timestamp, namespace, entity, limit));
    }

    List<Document> doFindAll(ReadTransaction transaction, ZonedDateTime timestamp, String namespace, String entity, int limit) {
        final DirectorySubspace primary = getPrimary(namespace, entity);
        /*
         * Get all fragments of all versions.
         */
        final AsyncIterable<KeyValue> range = transaction.getRange(
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from())),
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from("~")))
        );

        final Map<String, Document> documentById = new ConcurrentHashMap<>();
        final List<Fragment> fragments = new ArrayList<>();
        final AsyncIterator<KeyValue> iterator = range.iterator();
        CompletableFuture<List<Document>> documentsCompletableFuture = new CompletableFuture<>();
        iterator.onHasNext().thenAccept(onNextFragment(toTuple(timestamp), namespace, entity, null, primary, documentById, fragments, iterator, documentsCompletableFuture, null, limit));

        return documentsCompletableFuture.join();
    }

    private Consumer<Boolean> onNextFragment(Tuple timestampTuple, String namespace, String entity, String id, DirectorySubspace primary, Map<String, Document> documentById, List<Fragment> fragments, AsyncIterator<KeyValue> iterator, CompletableFuture<List<Document>> documentsCompletableFuture, Tuple prevVersion, int limit) {
        return hasNext -> {
            try {
                if (hasNext) {
                    KeyValue kv = iterator.next();
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
                                    document = new Document(namespace, entity, id, toTimestamp(prevVersion), Collections.emptyList(), true);
                                } else {
                                    document = new Document(namespace, entity, id, toTimestamp(prevVersion), List.copyOf(fragments), false);
                                }
                                documentById.put(id, document); // overwrite any earlier versions
                            }
                            fragments.clear();
                        }
                    }
                    fragments.add(new Fragment(path, value));
                    if (documentById.size() < limit) {
                        iterator.onHasNext().thenAccept(onNextFragment(timestampTuple, namespace, entity, dbId, primary, documentById, fragments, iterator, documentsCompletableFuture, versionTuple, limit));
                    } else {
                        // limit reached
                        documentsCompletableFuture.complete(new ArrayList<>(documentById.values()));
                        iterator.cancel();
                    }
                } else {
                    if (id != null && prevVersion.compareTo(timestampTuple) <= 0) {
                        Document document;
                        if (DELETED_MARKER.equals(fragments.get(0).getPath())) {
                            document = new Document(namespace, entity, id, toTimestamp(prevVersion), Collections.emptyList(), true);
                        } else {
                            document = new Document(namespace, entity, id, toTimestamp(prevVersion), List.copyOf(fragments), false);
                        }
                        documentById.put(id, document);
                    }
                    documentsCompletableFuture.complete(new ArrayList<>(documentById.values()));
                }
            } catch (Throwable t) {
                documentsCompletableFuture.completeExceptionally(t);
            }
        };
    }

    @Override
    public List<Document> find(ZonedDateTime timestamp, String namespace, String entity, String path, String value, int limit) throws PersistenceException {
        return db.read(transaction -> doFind(transaction, timestamp, namespace, entity, path, value, limit));
    }

    List<Document> doFind(ReadTransaction transaction, ZonedDateTime timestamp, String namespace, String entity, String path, String value, int limit) {
        // TODO use limit
        String arrayIndexUnawarePath = path.replaceAll(Fragment.arrayIndexPattern.pattern(), "[]");
        DirectorySubspace primary = getPrimary(namespace, entity);
        DirectorySubspace index = getIndex(namespace, entity, arrayIndexUnawarePath);
        Tuple timestampTuple = toTuple(timestamp);
        String truncatedValue = truncateToMaxKeyLength(value);
        AsyncIterable<KeyValue> range = transaction.getRange(
                KeySelector.firstGreaterOrEqual(index.pack(Tuple.from(truncatedValue))),
                KeySelector.firstGreaterOrEqual(index.pack(Tuple.from(truncatedValue, tick(timestampTuple))))
        );
        AsyncIterator<KeyValue> rangeIterator = range.iterator();
        List<Document> documents = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<Document>> documentFutures = new ArrayList<>();
        Map<String, SortedSet<Tuple>> matchingVersionsById = new LinkedHashMap<>();
        while (rangeIterator.hasNext()) {
            KeyValue kv = rangeIterator.next();
            if (kv.getValue().length > 0 && !value.equals(new String(kv.getValue(), StandardCharsets.UTF_8))) {
                // false-positive match due to value truncation
                continue;
            }
            Tuple key = index.unpack(kv.getKey());
            Tuple matchedVersion = key.getNestedTuple(1);
            String id = key.getString(3);
            matchingVersionsById.computeIfAbsent(id, k -> new TreeSet<>()).add(matchedVersion);
        }
        for (Map.Entry<String, SortedSet<Tuple>> entry : matchingVersionsById.entrySet()) {
            String id = entry.getKey();
            Tuple newestMatchingVersion = entry.getValue().last();
            documentFutures.add(findAnyOneMatchingFragmentInPrimary(transaction, primary, id, timestamp).thenCompose(kv -> {
                Tuple keyTuple = primary.unpack(kv.getKey());
                Tuple versionTuple = keyTuple.getNestedTuple(1);
                if (!newestMatchingVersion.equals(versionTuple)) {
                    return null; // false-positive index-match on older version
                }
                if (DELETED_MARKER.equals(keyTuple.getString(2))) {
                    // TODO Consider queuing best-effort task for removal of fragment from index that match path, value, timestamp, and id.
                    return CompletableFuture.completedFuture(new Document(namespace, entity, id, toTimestamp(versionTuple), Collections.emptyList(), true));
                }
                return getDocument(transaction, namespace, entity, id, versionTuple).thenApply(doc -> {
                    if (doc == null) {
                        // TODO Consider queuing best-effort task for removal of fragment from index that match path, value, timestamp, and id.
                        return null;
                    }
                    documents.add(doc);
                    return doc;
                });
            }));
        }

        CompletableFuture<Void> all = CompletableFuture.allOf(documentFutures.toArray(new CompletableFuture[documentFutures.size()]));
        all.join(); // wait for all documents to load asynchronously

        return documents;
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
