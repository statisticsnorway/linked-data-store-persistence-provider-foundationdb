package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.PersistenceResult;
import no.ssb.lds.api.persistence.PersistenceStatistics;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static no.ssb.lds.api.persistence.Fragment.DELETED_MARKER;

public class FoundationDBPersistence implements Persistence {

    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    static final ZoneId ZONE_ID_UTC = ZoneId.of("Etc/UTC");
    static final int CHUNK_SIZE = 8 * 1024; // FoundationDB hard-limit is 100 KB.
    static final int MAX_DESIRED_KEY_LENGTH = 256; // FoundationDB hard-limit is 10 KB.

    static final String PRIMARY_INDEX = "Primary";
    static final String PATH_VALUE_INDEX = "PathValueIndex";

    final Database db;
    final Directory directory;
    final Map<Tuple, CompletableFuture<DirectorySubspace>> directorySubspaceByPaths = new ConcurrentHashMap<>();

    public FoundationDBPersistence(Database db, Directory directory) {
        this.db = db;
        this.directory = directory;
    }

    /**
     * Directory: (NAMESPACE, "Primary", ENTITY)
     * Primary: (ID, (-TIMESTAMP), PATH, OFFSET)  =  VALUE
     *
     * @param namespace
     * @param entity
     * @return
     */
    CompletableFuture<DirectorySubspace> getPrimary(String namespace, String entity) {
        return createOrOpenDirectorySubspace(Tuple.from(namespace, PRIMARY_INDEX, entity));
    }

    /**
     * Directory: (NAMESPACE, "PathIndex", ENTITY, ARRAY-INDEX-UNAWARE-PATH)
     * PathIndex: (VALUE, ID, (-TIMESTAMP), ARRAY-INDICES-FROM-PATH) = EMPTY
     *
     * @param namespace
     * @param entity
     * @param path
     * @return
     */
    CompletableFuture<DirectorySubspace> getIndex(String namespace, String entity, String path) {
        return createOrOpenDirectorySubspace(Tuple.from(namespace, PATH_VALUE_INDEX, entity, path));
    }

    CompletableFuture<DirectorySubspace> createOrOpenDirectorySubspace(Tuple key) {
        // To create a nested subdirectory per tuple item, use: directory.createOrOpen(db, t.stream().map(o -> (String) o).collect(Collectors.toList())
        return directorySubspaceByPaths.computeIfAbsent(key, k -> directory.createOrOpen(db, List.of(k.toString())));
    }

    @Override
    public CompletableFuture<PersistenceStatistics> createOrOverwrite(Flow.Publisher<Fragment> publisher) throws PersistenceException {
        CompletableFuture<PersistenceStatistics> result = new CompletableFuture<>();
        publisher.subscribe(new CreateOrOverwriteSubscriber(this, result, new FoundationDBStatistics()));
        return result;
    }

    @Override
    public Flow.Publisher<PersistenceResult> read(ZonedDateTime snapshot, String namespace, String entity, String id) throws PersistenceException {
        return new ReadPublisher(this, new FoundationDBStatistics(), toTuple(snapshot), namespace, entity, id);
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
    static CompletableFuture<KeyValue> findAnyOneMatchingFragmentInPrimary(FoundationDBStatistics statistics, ReadTransaction transaction, DirectorySubspace primary, String id, Tuple snapshot) {
        /*
         * The range specified is guaranteed to never return more than 1 result. The returned KeyValue list will be one of:
         *   (1) Last fragment of matching resource when resource exists and client-timestamp is greater than or equal to resource timestamp
         *   (2) Last fragment of an unrelated resource when resource does not exist for the specified timestamp
         *   (3) KeyValue of another key-space than PRIMARYDBSubscription subscription, Tuple snapshot, ReadTransaction transaction, FoundationDBStatistics statistics, String namespace, String entity, String id, Tuple version, int limit) {
        CompletableFuture<PersistenceResult> result =
         *   (4) Empty when database is empty (or an unlikely corner-case when asking for a resource at beginning of key-space)
         */

        AsyncIterable<KeyValue> version = transaction.getRange(
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, snapshot))),
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id + " "))),
                1,
                false,
                StreamingMode.EXACT
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

    void getDocument(FoundationDBSubscription subscription, Tuple snapshot, ReadTransaction transaction, FoundationDBStatistics statistics, String namespace, String entity, String id, Tuple version, int limit) {
        getPrimary(namespace, entity).thenAccept(primary -> {
            AsyncIterable<KeyValue> range = transaction.getRange(primary.range(Tuple.from(id, version)));
            statistics.getRange(PRIMARY_INDEX);
            AsyncIterator<KeyValue> iterator = range.iterator();
            iterator.onHasNext().thenAccept(new PrimaryIterator(subscription, snapshot, statistics, namespace, entity, null, primary, iterator, limit));
        });
    }

    @Override
    public Flow.Publisher<PersistenceResult> readVersions(ZonedDateTime snapshotFrom, ZonedDateTime snapshotTo, String namespace, String entity, String id, int limit) throws PersistenceException {
        return new ReadVersionsPublisher(this, new FoundationDBStatistics(), toTuple(snapshotFrom), toTuple(snapshotTo), namespace, entity, id, limit);
    }

    @Override
    public Flow.Publisher<PersistenceResult> readAllVersions(String namespace, String entity, String id, int limit) throws PersistenceException {
        return new ReadAllVersionsPublisher(this, new FoundationDBStatistics(), namespace, entity, id, limit);
    }

    @Override
    public CompletableFuture<PersistenceStatistics> delete(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        return db.runAsync(transaction -> doDelete(transaction, new FoundationDBStatistics(), toTuple(timestamp), namespace, entity, id, policy));
    }

    private CompletableFuture<PersistenceStatistics> doDelete(Transaction transaction, FoundationDBStatistics statistics, Tuple timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) {
        CompletableFuture<PersistenceStatistics> result = new CompletableFuture<>();

        getPrimary(namespace, entity).thenAccept(primary -> {

            /*
             * Get all fragments of given versioned resource.
             */
            Range range = primary.range(Tuple.from(id, timestamp));
            AsyncIterator<KeyValue> iterator = transaction.getRange(range).iterator();
            statistics.getRange(PRIMARY_INDEX);

            CompletableFuture<PersistenceStatistics> fragmentsDeletedSignal = new CompletableFuture<>();
            iterator.onHasNext().thenAccept(doDeleteAllIndexFragments(fragmentsDeletedSignal, transaction, statistics, namespace, entity, primary, iterator));

            fragmentsDeletedSignal.thenAccept(v -> {
                transaction.clear(range);
                statistics.clearRange(PRIMARY_INDEX);
                result.complete(statistics);
            });
        });

        return result;
    }

    @Override
    public CompletableFuture<PersistenceStatistics> deleteAllVersions(String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        return db.runAsync(transaction -> doDeleteAllVersions(transaction, new FoundationDBStatistics(), namespace, entity, id, policy));
    }

    private CompletableFuture<PersistenceStatistics> doDeleteAllVersions(Transaction transaction, FoundationDBStatistics statistics, String namespace, String entity, String id, PersistenceDeletePolicy policy) {
        CompletableFuture<PersistenceStatistics> result = new CompletableFuture<>();

        getPrimary(namespace, entity).thenAccept(primary -> {

            /*
             * Get all fragments of all versions of resource.
             */
            Range range = primary.range(Tuple.from(id));
            AsyncIterator<KeyValue> iterator = transaction.getRange(range).iterator();
            statistics.getRange(PRIMARY_INDEX);

            CompletableFuture<PersistenceStatistics> fragmentsDeletedSignal = new CompletableFuture<>();
            iterator.onHasNext().thenAccept(doDeleteAllIndexFragments(fragmentsDeletedSignal, transaction, statistics, namespace, entity, primary, iterator));

            fragmentsDeletedSignal.thenAccept(v -> {
                transaction.clear(range);
                statistics.clearRange(PRIMARY_INDEX);
                result.complete(statistics);
            });
        });

        return result;
    }

    private Consumer<Boolean> doDeleteAllIndexFragments(CompletableFuture<PersistenceStatistics> fragmentsDeletedSignal, Transaction transaction, FoundationDBStatistics statistics, String namespace, String entity, DirectorySubspace primary, AsyncIterator<KeyValue> iterator) {
        return hasNext -> {
            try {
                if (!hasNext) {
                    fragmentsDeletedSignal.complete(statistics);
                    return;
                }

                KeyValue kv = iterator.next();
                Tuple key = primary.unpack(kv.getKey());
                String id = key.getString(0);
                Tuple version = key.getNestedTuple(1);
                String path = key.getString(2);
                long offset = key.getLong(3);

                if (offset > 0) {
                    iterator.onHasNext().thenAccept(doDeleteAllIndexFragments(fragmentsDeletedSignal, transaction, statistics, namespace, entity, primary, iterator));
                    return;
                }

                Fragment fragment = new Fragment(namespace, entity, id, toTimestamp(version), path, 0, kv.getValue());
                ArrayList<Integer> indices = new ArrayList<>();
                String indexUnawarePath = Fragment.computeIndexUnawarePath(path, indices);
                Tuple arrayIndices = Tuple.from(indices);
                String truncatedValue = fragment.truncatedValue();
                Tuple valueIndexKey = Tuple.from(
                        truncatedValue,
                        id,
                        version,
                        arrayIndices
                );
                getIndex(namespace, entity, indexUnawarePath).thenAccept(index -> {
                    byte[] binaryValueIndexKey = index.pack(valueIndexKey);
                    transaction.clear(binaryValueIndexKey);
                    statistics.clearKeyValue(PATH_VALUE_INDEX);
                    iterator.onHasNext().thenAccept(doDeleteAllIndexFragments(fragmentsDeletedSignal, transaction, statistics, namespace, entity, primary, iterator));
                });
            } catch (Throwable t) {
                fragmentsDeletedSignal.completeExceptionally(t);
            }
        };
    }


    @Override
    public CompletableFuture<PersistenceStatistics> markDeleted(ZonedDateTime timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        return db.runAsync(transaction -> doMarkDeleted(transaction, new FoundationDBStatistics(), toTuple(timestamp), namespace, entity, id, policy));
    }

    CompletableFuture<PersistenceStatistics> doMarkDeleted(Transaction transaction, FoundationDBStatistics statistics, Tuple timestamp, String namespace, String entity, String id, PersistenceDeletePolicy policy) {
        return getPrimary(namespace, entity).thenCompose(primary -> {

            // Clear primary of existing document with same version
            transaction.clear(primary.range(Tuple.from(id, timestamp)));
            statistics.clearRange(PRIMARY_INDEX);

            /*
             * PRIMARY
             */
            Tuple primaryKey = Tuple.from(
                    id,
                    timestamp,
                    DELETED_MARKER,
                    0
            );
            byte[] binaryPrimaryKey = primary.pack(primaryKey);
            transaction.set(binaryPrimaryKey, EMPTY_BYTE_ARRAY);
            statistics.setKeyValue(PRIMARY_INDEX);

            return CompletableFuture.completedFuture(statistics);
        });
    }

    @Override
    public Flow.Publisher<PersistenceResult> findAll(ZonedDateTime snapshot, String namespace, String entity, int limit) throws PersistenceException {
        return new FindAllPublisher(this, new FoundationDBStatistics(), toTuple(snapshot), namespace, entity, limit);
    }

    @Override
    public Flow.Publisher<PersistenceResult> find(ZonedDateTime snapshot, String namespace, String entity, String path, String value, int limit) throws PersistenceException {
        return new FindPublisher(this, new FoundationDBStatistics(), toTuple(snapshot), namespace, entity, path, value, limit);
    }

    @Override
    public void close() throws PersistenceException {
        db.close();
    }

    static Tuple toTuple(ZonedDateTime timestamp) {
        return Tuple.from(
                -timestamp.getYear(),
                -timestamp.getMonth().getValue(),
                -timestamp.getDayOfMonth(),
                -timestamp.getHour(),
                -timestamp.getMinute(),
                -timestamp.getSecond(),
                -TimeUnit.NANOSECONDS.toMillis(timestamp.getNano())
        );
    }

    static ZonedDateTime toTimestamp(Tuple timestampTuple) {
        return ZonedDateTime.of(
                (int) -timestampTuple.getLong(0),
                (int) -timestampTuple.getLong(1),
                (int) -timestampTuple.getLong(2),
                (int) -timestampTuple.getLong(3),
                (int) -timestampTuple.getLong(4),
                (int) -timestampTuple.getLong(5),
                (int) -TimeUnit.MILLISECONDS.toNanos(timestampTuple.getLong(6)),
                ZONE_ID_UTC
        );
    }

    static Tuple tickForward(Tuple documentTimestampTuple) {
        return documentTimestampTuple.popBack().add(documentTimestampTuple.getLong(6) - 1);
    }

    static Tuple tickBackward(Tuple documentTimestampTuple) {
        return documentTimestampTuple.popBack().add(documentTimestampTuple.getLong(6) + 1);
    }
}
