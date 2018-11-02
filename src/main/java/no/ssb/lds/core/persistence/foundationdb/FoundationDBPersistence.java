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
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Document;
import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/*
 * PRIMARY:
 *
 *   / NAMESPACE / ENTITY / ID / TIMESTAMP / PATH  =  VALUE
 *
 *
 * VALUE-INDEX:
 *
 *   / NAMESPACE / ENTITY / ARRAY-INDEX-UNAWARE-PATH / VALUE / TIMESTAMP / ARRAY-INDICES-FROM-PATH / ID  =  ""
 *
 */

public class FoundationDBPersistence implements Persistence {

    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    static final ZoneId ZONE_ID_UTC = ZoneId.of("Etc/UTC");

    final Database db;
    final Directory namespaceDirectory;
    final Map<String, Map<String, DirectorySubspace>> directorySubspaceByDomainByNamespace = new ConcurrentHashMap<>();

    public FoundationDBPersistence(Database db, String defaultNamespace, Directory namespaceDirectory, Map<String, DirectorySubspace> defaultNamespaceDirectorySubspaceByDomain) {
        this.db = db;
        this.namespaceDirectory = namespaceDirectory;
    }

    private DirectorySubspace getPrimary(String namespace, String entity) {
        return directorySubspaceByDomainByNamespace.computeIfAbsent(namespace, ns -> new ConcurrentHashMap<>()).computeIfAbsent(entity, createOrOpenDirectorySubspace());
    }

    private DirectorySubspace getIndex(String namespace, String entity) {
        return directorySubspaceByDomainByNamespace.computeIfAbsent(namespace, ns -> new ConcurrentHashMap<>()).computeIfAbsent("Index-" + entity, createOrOpenDirectorySubspace());
    }

    private Function<String, DirectorySubspace> createOrOpenDirectorySubspace() {
        return subspace -> {
            try {
                return namespaceDirectory.createOrOpen(db, PathUtil.from(subspace)).get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw coerceRuntimeCause(e);
            }
        };
    }

    @Override
    public void createOrOverwrite(Document document) throws PersistenceException {
        db.run(transaction -> doCreateOrOverwrite(transaction, document));
    }

    Object doCreateOrOverwrite(Transaction transaction, Document document) {
        // TODO clear version in db first.

        DirectorySubspace primary = getPrimary(document.getNamespace(), document.getEntity());
        DirectorySubspace index = getIndex(document.getNamespace(), document.getEntity());

        for (Fragment fragment : document.getFragments()) {

            Tuple timestampTuple = toTuple(document.getTimestamp());

            /*
             * PRIMARY
             */
            Tuple primaryKey = Tuple.from(
                    document.getId(),
                    timestampTuple,
                    fragment.getPath()
            );
            Tuple primaryValue = Tuple.from(fragment.getValue());
            byte[] binaryPrimaryKey = primary.pack(primaryKey);
            byte[] binaryPrimaryValue = primaryValue.pack();
            transaction.set(binaryPrimaryKey, binaryPrimaryValue);

            /*
             * INDEX
             */
            Tuple valueIndexKey = Tuple.from(
                    fragment.getArrayIndicesUnawarePath(),
                    fragment.getValue(), // TODO truncate value to max-length (approx 9KB) and support this in querying
                    timestampTuple,
                    Tuple.from(fragment.getArrayIndices()),
                    document.getId()
            );
            byte[] binaryValueIndexKey = index.pack(valueIndexKey);
            transaction.set(binaryValueIndexKey, EMPTY_BYTE_ARRAY);

        }

        return null;
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

            /*
             * Get document with given version.
             */
            CompletableFuture<Document> document = getDocument(transaction, namespace, entity, id, version);
            try {
                return document.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw coerceRuntimeCause(e);
            }
        });
        try {
            return matchingDocument.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw coerceRuntimeCause(e);
        }
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
            if (hasNext) {
                KeyValue kv = iterator.next();
                Tuple keyTuple = primary.unpack(kv.getKey());
                String value = Tuple.fromBytes(kv.getValue()).getString(0);
                String path = keyTuple.getString(2);
                fragments.add(new Fragment(path, value));
                iterator.onHasNext().thenAccept(getBooleanConsumer(iterator, documentCompletableFuture, fragments, primary, namespace, entity, id, version));
            } else {
                Document document = new Document(namespace, entity, id, toTimestamp(version), fragments);
                documentCompletableFuture.complete(document);
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
        return db.read(transaction -> {
            try {
                return doReadVersions(transaction, from, to, namespace, entity, id, limit).get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw coerceRuntimeCause(e);
            }
        });
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
            try {
                return documents.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw coerceRuntimeCause(e);
            }
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
            if (hasNext) {
                KeyValue kv = iterator.next();
                Tuple keyTuple = primary.unpack(kv.getKey());
                String value = Tuple.fromBytes(kv.getValue()).getString(0);
                String dbId = keyTuple.getString(0);
                String path = keyTuple.getString(2);
                Tuple timestampTuple = keyTuple.getNestedTuple(1);
                if (id != null) {
                    if (!id.equals(dbId) || prevVersionRef.get() != null && !timestampTuple.equals(prevVersionRef.get())) {
                        documents.add(new Document(namespace, entity, id, toTimestamp(prevVersionRef.get()), List.copyOf(fragments)));
                        fragments.clear();
                    }
                }
                fragments.add(new Fragment(path, value));
                prevVersionRef.set(keyTuple.getNestedTuple(1));
                iterator.onHasNext().thenAccept(onNextFragment(namespace, entity, dbId, primary, documents, fragments, iterator, documentsCompletableFuture, prevVersionRef));
            } else {
                if (id != null) {
                    documents.add(new Document(namespace, entity, id, toTimestamp(prevVersionRef.get()), List.copyOf(fragments)));
                }
                documentsCompletableFuture.complete(documents);
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
        try {
            return documents.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw coerceRuntimeCause(e);
        }
    }

    @Override
    public boolean delete(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        return false;
    }

    @Override
    public boolean markDeleted(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        return false;
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

        try {
            return documentsCompletableFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw coerceRuntimeCause(e);
        }
    }

    private Consumer<Boolean> onNextFragment(Tuple timestampTuple, String namespace, String entity, String id, DirectorySubspace primary, Map<String, Document> documentById, List<Fragment> fragments, AsyncIterator<KeyValue> iterator, CompletableFuture<List<Document>> documentsCompletableFuture, Tuple prevVersion, int limit) {
        return hasNext -> {
            if (hasNext) {
                KeyValue kv = iterator.next();
                Tuple keyTuple = primary.unpack(kv.getKey());
                String value = Tuple.fromBytes(kv.getValue()).getString(0);
                String dbId = keyTuple.getString(0);
                String path = keyTuple.getString(2);
                Tuple versionTuple = keyTuple.getNestedTuple(1);
                if (id != null) {
                    if (!id.equals(dbId) || (prevVersion != null && !versionTuple.equals(prevVersion))) {
                        if (prevVersion.compareTo(timestampTuple) <= 0) {
                            documentById.put(id, new Document(namespace, entity, id, toTimestamp(prevVersion), List.copyOf(fragments)));
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
                    documentById.put(id, new Document(namespace, entity, id, toTimestamp(prevVersion), List.copyOf(fragments)));
                }
                documentsCompletableFuture.complete(new ArrayList<>(documentById.values()));
            }
        };
    }

    @Override
    public List<Document> find(ZonedDateTime timestamp, String namespace, String entity, String path, String value, int limit) throws PersistenceException {
        return db.read(transaction -> doFind(transaction, timestamp, namespace, entity, path, value, limit));
    }

    List<Document> doFind(ReadTransaction transaction, ZonedDateTime timestamp, String namespace, String entity, String path, String value, int limit) {
        // TODO use limit
        DirectorySubspace index = getIndex(namespace, entity);
        String arrayIndexUnawarePath = path.replaceAll(Fragment.arrayIndexPattern.pattern(), "[]");
        Tuple timestampTuple = toTuple(timestamp);
        AsyncIterable<KeyValue> range = transaction.getRange(
                KeySelector.firstGreaterOrEqual(index.pack(Tuple.from(arrayIndexUnawarePath, value))),
                KeySelector.firstGreaterOrEqual(index.pack(Tuple.from(arrayIndexUnawarePath, value, tick(timestampTuple))))
        );
        AsyncIterator<KeyValue> rangeIterator = range.iterator();
        List<Document> documents = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<Document>> documentFutures = new ArrayList<>();
        Map<String, SortedSet<Tuple>> matchingVersionsById = new LinkedHashMap<>();
        while (rangeIterator.hasNext()) {
            // / NAMESPACE / ENTITY / ARRAY-INDEX-UNAWARE-PATH / VALUE / TIMESTAMP / ARRAY-INDICES-FROM-PATH / ID  =  ""
            KeyValue kv = rangeIterator.next();
            Tuple key = Tuple.fromBytes(kv.getKey());
            Tuple matchedVersion = key.getNestedTuple(4);
            String id = key.getString(6);
            matchingVersionsById.computeIfAbsent(id, k -> new TreeSet<>()).add(matchedVersion);
        }
        for (Map.Entry<String, SortedSet<Tuple>> entry : matchingVersionsById.entrySet()) {
            String id = entry.getKey();
            Tuple newestMatchingVersion = entry.getValue().last();
            documentFutures.add(getDocument(transaction, namespace, entity, id, newestMatchingVersion).thenApply(doc -> {
                documents.add(doc);
                return doc;
            }));
        }

        CompletableFuture<Void> all = CompletableFuture.allOf(documentFutures.toArray(new CompletableFuture[documentFutures.size()]));
        try {
            all.get(); // wait for all documents to load asynchronously
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw coerceRuntimeCause(e);
        }

        return documents;
    }

    @Override
    public void close() throws PersistenceException {
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

    static RuntimeException coerceRuntimeCause(ExecutionException e) {
        if (e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
        }
        if (e.getCause() instanceof Error) {
            throw (Error) e.getCause();
        }
        throw new RuntimeException(e.getCause());
    }
}
