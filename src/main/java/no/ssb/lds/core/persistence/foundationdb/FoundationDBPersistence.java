package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.Persistence;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
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

    final TransactionFactory transactionFactory;
    final FoundationDBDirectory foundationDBDirectory;

    protected FoundationDBPersistence(TransactionFactory transactionFactory, FoundationDBDirectory foundationDBDirectory) {
        this.transactionFactory = transactionFactory;
        this.foundationDBDirectory = foundationDBDirectory;
    }

    /**
     * Directory: (NAMESPACE, "Primary", ENTITY)
     * Primary: (ID, (-TIMESTAMP), PATH, OFFSET)  =  VALUE
     *
     * @param namespace
     * @param entity
     * @return
     */
    CompletableFuture<? extends Subspace> getPrimary(String namespace, String entity) {
        return foundationDBDirectory.createOrOpen(Tuple.from(namespace, PRIMARY_INDEX, entity));
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
    CompletableFuture<? extends Subspace> getIndex(String namespace, String entity, String path) {
        return foundationDBDirectory.createOrOpen(Tuple.from(namespace, PATH_VALUE_INDEX, entity, path));
    }

    @Override
    public TransactionFactory transactionFactory() throws PersistenceException {
        return transactionFactory;
    }

    @Override
    public Transaction createTransaction(boolean readOnly) throws PersistenceException {
        return transactionFactory.createTransaction(readOnly);
    }

    @Override
    public CompletableFuture<Void> createOrOverwrite(Transaction transaction, Flow.Publisher<Fragment> publisher) throws PersistenceException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        publisher.subscribe(new CreateOrOverwriteSubscriber(this, result, (OrderedKeyValueTransaction) transaction));
        return result;
    }

    @Override
    public Flow.Publisher<Fragment> read(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String id) throws PersistenceException {
        return new ReadPublisher(this, (OrderedKeyValueTransaction) transaction, toTuple(snapshot), namespace, entity, id);
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
    static CompletableFuture<KeyValue> findAnyOneMatchingFragmentInPrimary(OrderedKeyValueTransaction transaction, Subspace primary, String id, Tuple snapshot) {
        /*
         * The range specified is guaranteed to never return more than 1 result. The returned KeyValue list will be one of:
         *   (1) Last fragment of matching resource when resource exists and client-timestamp is greater than or equal to resource timestamp
         *   (2) Last fragment of an unrelated resource when resource does not exist for the specified timestamp
         *   (3) KeyValue of another key-space than PRIMARYDBSubscription subscription, Tuple snapshot, ReadTransaction transaction, FoundationDBStatistics statistics, String namespace, String entity, String id, Tuple version, int limit) {
        CompletableFuture<FragmentResult> result =
         *   (4) Empty when database is empty (or an unlikely corner-case when asking for a resource at beginning of key-space)
         */

        AsyncIterable<KeyValue> version = transaction.getRange(
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, snapshot))),
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id + " "))),
                1,
                StreamingMode.EXACT,
                PRIMARY_INDEX
        );

        return version.asList().thenApply(keyValues -> {
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
    public Flow.Publisher<Fragment> readVersions(Transaction transaction, ZonedDateTime snapshotFrom, ZonedDateTime snapshotTo, String namespace, String entity, String id, String firstId, int limit) throws PersistenceException {
        return new ReadVersionsPublisher(this, (OrderedKeyValueTransaction) transaction, toTuple(snapshotFrom), toTuple(snapshotTo), namespace, entity, id, limit);
    }

    @Override
    public Flow.Publisher<Fragment> readAllVersions(Transaction transaction, String namespace, String entity, String id, ZonedDateTime firstVersion, int limit) throws PersistenceException {
        return new ReadAllVersionsPublisher(this, (OrderedKeyValueTransaction) transaction, namespace, entity, id, limit);
    }

    @Override
    public CompletableFuture<Void> delete(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
        return doDelete((OrderedKeyValueTransaction) transaction, namespace, entity, id, toTuple(version), policy);
    }

    private CompletableFuture<Void> doDelete(OrderedKeyValueTransaction transaction, String namespace, String entity, String id, Tuple version, PersistenceDeletePolicy policy) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        getPrimary(namespace, entity).thenAccept(primary -> {

            /*
             * Get all fragments of given versioned resource.
             */
            Range range = primary.range(Tuple.from(id, version));
            AsyncIterator<KeyValue> iterator = transaction.getRange(range, PRIMARY_INDEX).iterator();

            CompletableFuture<Void> fragmentsDeletedSignal = new CompletableFuture<>();
            iterator.onHasNext().thenAccept(doDeleteAllIndexFragments(fragmentsDeletedSignal, transaction, namespace, entity, primary, iterator));

            fragmentsDeletedSignal.thenAccept(v -> {
                transaction.clearRange(range, PRIMARY_INDEX);
                result.complete(null);
            });
        });

        return result;
    }

    @Override
    public CompletableFuture<Void> deleteAllVersions(Transaction transaction, String namespace, String entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        return doDeleteAllVersions((OrderedKeyValueTransaction) transaction, namespace, entity, id, policy);
    }

    private CompletableFuture<Void> doDeleteAllVersions(OrderedKeyValueTransaction transaction, String namespace, String entity, String id, PersistenceDeletePolicy policy) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        getPrimary(namespace, entity).thenAccept(primary -> {

            /*
             * Get all fragments of all versions of resource.
             */
            Range range = primary.range(Tuple.from(id));
            AsyncIterator<KeyValue> iterator = transaction.getRange(range, PRIMARY_INDEX).iterator();

            CompletableFuture<Void> fragmentsDeletedSignal = new CompletableFuture<>();
            iterator.onHasNext().thenAccept(doDeleteAllIndexFragments(fragmentsDeletedSignal, transaction, namespace, entity, primary, iterator));

            fragmentsDeletedSignal.thenAccept(v -> {
                transaction.clearRange(range, PRIMARY_INDEX);
                result.complete(null);
            });
        });

        return result;
    }

    private Consumer<Boolean> doDeleteAllIndexFragments(CompletableFuture<Void> fragmentsDeletedSignal, OrderedKeyValueTransaction transaction, String namespace, String entity, Subspace primary, AsyncIterator<KeyValue> iterator) {
        return hasNext -> {
            try {
                if (!hasNext) {
                    fragmentsDeletedSignal.complete(null);
                    return;
                }

                KeyValue kv = iterator.next();
                Tuple key = primary.unpack(kv.getKey());
                String id = key.getString(0);
                Tuple version = key.getNestedTuple(1);
                String path = key.getString(2);
                long offset = key.getLong(3);

                if (offset > 0) {
                    iterator.onHasNext().thenAccept(doDeleteAllIndexFragments(fragmentsDeletedSignal, transaction, namespace, entity, primary, iterator));
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
                    transaction.clear(binaryValueIndexKey, PATH_VALUE_INDEX);
                    iterator.onHasNext().thenAccept(doDeleteAllIndexFragments(fragmentsDeletedSignal, transaction, namespace, entity, primary, iterator));
                });
            } catch (Throwable t) {
                fragmentsDeletedSignal.completeExceptionally(t);
            }
        };
    }


    @Override
    public CompletableFuture<Void> markDeleted(Transaction transaction, String namespace, String entity, String id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
        return doMarkDeleted((OrderedKeyValueTransaction) transaction, toTuple(version), namespace, entity, id, policy);
    }

    CompletableFuture<Void> doMarkDeleted(OrderedKeyValueTransaction transaction, Tuple version, String namespace, String entity, String id, PersistenceDeletePolicy policy) {
        return getPrimary(namespace, entity).thenCompose(primary -> {

            // Clear primary of existing document with same version
            transaction.clearRange(primary.range(Tuple.from(id, version)), PRIMARY_INDEX);

            /*
             * PRIMARY
             */
            Tuple primaryKey = Tuple.from(
                    id,
                    version,
                    DELETED_MARKER,
                    0
            );
            byte[] binaryPrimaryKey = primary.pack(primaryKey);
            transaction.set(binaryPrimaryKey, EMPTY_BYTE_ARRAY, PRIMARY_INDEX);

            return CompletableFuture.completedFuture(null);
        });
    }

    @Override
    public Flow.Publisher<Fragment> findAll(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String firstId, int limit) throws PersistenceException {
        return new FindAllPublisher(this, (OrderedKeyValueTransaction) transaction, toTuple(snapshot), namespace, entity, limit);
    }

    @Override
    public Flow.Publisher<Fragment> find(Transaction transaction, ZonedDateTime snapshot, String namespace, String entity, String path, String value, String firstId, int limit) throws PersistenceException {
        return new FindByPathAndValuePublisher(this, (OrderedKeyValueTransaction) transaction, toTuple(snapshot), namespace, entity, path, value, limit);
    }

    @Override
    public void close() throws PersistenceException {
        transactionFactory.close();
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
