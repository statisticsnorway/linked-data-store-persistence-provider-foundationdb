package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceException;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionFactory;
import no.ssb.lds.api.persistence.reactivex.RxPersistence;
import no.ssb.lds.api.persistence.streaming.Fragment;
import no.ssb.lds.api.persistence.streaming.FragmentType;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.foundationdb.ReadTransaction.ROW_LIMIT_UNLIMITED;
import static java.util.Optional.ofNullable;

public class FoundationDBRxPersistence implements RxPersistence {

    // FoundationDB value size hard-limit is 100 KB.
    // FoundationDB key size hard-limit is 10 KB.

    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    static final ZoneId ZONE_ID_UTC = ZoneId.of("Etc/UTC");
    static final int MAX_DESIRED_KEY_LENGTH = 256;

    public static final String PRIMARY_INDEX = "Primary";
    public static final String PATH_VALUE_INDEX = "PathValueIndex";

    private static final ZonedDateTime BEGINNING_OF_TIME = ZonedDateTime.of(1, 1, 1, 0, 0, 0, 0, ZoneId.of("Etc/UTC"));
    private static final ZonedDateTime END_OF_TIME = ZonedDateTime.of(9999, 1, 1, 0, 0, 0, 0, ZoneId.of("Etc/UTC"));

    public static final KeyValue EMPTY = new KeyValue(new byte[0], new byte[0]);

    final TransactionFactory transactionFactory;
    final FoundationDBDirectory foundationDBDirectory;

    protected FoundationDBRxPersistence(TransactionFactory transactionFactory, FoundationDBDirectory foundationDBDirectory) {
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
    public Single<? extends Subspace> getPrimary(String namespace, String entity) {
        return Single.fromFuture(foundationDBDirectory.createOrOpen(Tuple.from(namespace, PRIMARY_INDEX, entity)));
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
    public Single<? extends Subspace> getIndex(String namespace, String entity, String path) {
        return Single.fromFuture(foundationDBDirectory.createOrOpen(Tuple.from(namespace, PATH_VALUE_INDEX, entity, path)));
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
    public Completable createOrOverwrite(Transaction tx, Flowable<Fragment> publisher) {
        // TODO add batch-size
        final OrderedKeyValueTransaction transaction = (OrderedKeyValueTransaction) tx;
        final CopyOnWriteArraySet<Range> clearedRanges = new CopyOnWriteArraySet<>();
        return publisher.flatMapCompletable(fragment -> getPrimary(fragment.namespace(), fragment.entity()).flatMapCompletable(primary -> {
            Tuple fragmentVersion = toTuple(fragment.timestamp());

            // Clear primary of existing document with same version
            Range range = primary.range(Tuple.from(fragment.id(), fragmentVersion));
            if (clearedRanges.add(range)) {
                transaction.clearRange(range, PRIMARY_INDEX);
            }

            // NOTE: With current implementation we do not need to clear the index. False-positive matches in the index
            // are always followed up by a primary lookup. Clearing Index space is expensive as it requires a read to
            // figure out whether there is anything to clear and then another read to get existing document and then finally
            // clearing each document fragment independently from the existing document in the index space which cannot be
            // done with a single range operation and therefore must be done using individual write operations per fragment.

            /*
             * PRIMARY
             */
            Tuple primaryKey = Tuple.from(
                    fragment.id(),
                    fragmentVersion,
                    fragment.path(),
                    new byte[]{fragment.fragmentType().getTypeCode()},
                    fragment.offset()
            );
            byte[] binaryPrimaryKey = primary.pack(primaryKey);
            transaction.set(binaryPrimaryKey, fragment.value(), PRIMARY_INDEX);

            /*
             * INDEX
             */
            ArrayList<Integer> arrayIndices = new ArrayList<>();
            String indexUnawarePath = Fragment.computeIndexUnawarePath(fragment.path(), arrayIndices);
            if (fragment.offset() == 0) {
                return getIndex(fragment.namespace(), fragment.entity(), indexUnawarePath).flatMapCompletable(index -> {
                    Tuple valueIndexKey = Tuple.from(
                            fragment.truncatedValue(),
                            fragment.id(),
                            fragmentVersion,
                            Tuple.from(arrayIndices)
                    );
                    byte[] binaryValueIndexKey = index.pack(valueIndexKey);
                    if (binaryValueIndexKey.length > MAX_DESIRED_KEY_LENGTH) {
                        throw new IllegalArgumentException("Document fragment key is too big for index, at most " + MAX_DESIRED_KEY_LENGTH + " bytes allowed. Was: " + binaryValueIndexKey.length + " bytes.");
                    }
                    transaction.set(binaryValueIndexKey, EMPTY_BYTE_ARRAY, PATH_VALUE_INDEX);
                    return Completable.complete();
                });
            }
            return Completable.complete();
        }));
    }

    @Override
    public Flowable<Fragment> read(Transaction tx, ZonedDateTime snapshot, String namespace, String entity, String id) {
        Tuple snapshotTuple = toTuple(snapshot);
        final OrderedKeyValueTransaction transaction = (OrderedKeyValueTransaction) tx;
        return Single.defer(() -> getPrimary(namespace, entity))
                .flatMapPublisher(primary -> findAnyOneMatchingFragmentInPrimary(transaction, primary, id, snapshotTuple)
                        .filter(kv -> primary.contains(kv.getKey()))
                        .flatMapPublisher(theMatchingFragmentInPrimary -> {
                            Tuple key = primary.unpack(theMatchingFragmentInPrimary.getKey());
                            Tuple version = key.getNestedTuple(1);
                            FragmentType fragmentType = FragmentType.fromTypeCode(key.getBytes(3)[0]);
                            if (FragmentType.DELETED.equals(fragmentType)) {
                                Fragment deleteMarkerFragment = new Fragment(namespace, entity, id, toTimestamp(version), "", FragmentType.DELETED, 0, EMPTY_BYTE_ARRAY);
                                return Single.just(deleteMarkerFragment).toFlowable();
                            }

                            /*
                             * Get document with given version.
                             */
                            AsyncIterable<KeyValue> asyncIterable = transaction.getRange(primary.range(Tuple.from(id, version)), PRIMARY_INDEX, ROW_LIMIT_UNLIMITED);
                            return Flowable.fromPublisher(new AsyncIterablePublisher<>(asyncIterable))
                                    .map(kv -> toFragment(primary, kv, namespace, entity));
                        })
                );
    }

    static Fragment toFragment(Subspace primary, KeyValue kv, String namespace, String entity) {
        Tuple keyTuple = primary.unpack(kv.getKey());
        String dbId = keyTuple.getString(0);
        Tuple version = keyTuple.getNestedTuple(1);
        String path = keyTuple.getString(2);
        byte fragmentTypeCode = keyTuple.getBytes(3)[0];
        FragmentType fragmentType = FragmentType.fromTypeCode(fragmentTypeCode);
        long offset = keyTuple.getLong(4);

        Fragment fragment = new Fragment(namespace, entity, dbId, toTimestamp(version), path, fragmentType, offset, kv.getValue());
        return fragment;
    }

    @Override
    public Flowable<Fragment> readVersions(Transaction tx, String namespace, String entity, String id, no.ssb.lds.api.persistence.reactivex.Range<ZonedDateTime> range) {
        Tuple snapshotFrom = toTuple(ofNullable(range.getAfter()).orElse(BEGINNING_OF_TIME));
        Tuple snapshotTo = toTuple(ofNullable(range.getBefore()).orElse(END_OF_TIME));
        final OrderedKeyValueTransaction transaction = (OrderedKeyValueTransaction) tx;
        return Single.defer(() -> getPrimary(namespace, entity))
                .flatMapPublisher(primary -> findAnyOneMatchingFragmentInPrimary(transaction, primary, id, snapshotFrom)
                        .filter(kv -> primary.contains(kv.getKey()))
                        .switchIfEmpty(Single.just(EMPTY))
                        .flatMapPublisher(aMatchingKeyValue -> {
                            Tuple lowerBound;
                            if (aMatchingKeyValue == EMPTY || !primary.contains(aMatchingKeyValue.getKey())) {
                                // no versions older or equals to snapshotFrom exists, use snapshotFrom as lower bound
                                lowerBound = snapshotFrom;
                            } else {
                                Tuple key = primary.unpack(aMatchingKeyValue.getKey());
                                lowerBound = tickBackward(key.getNestedTuple(1));
                            }

                            /*
                             * Get all fragments of all matching versions.
                             */
                            AsyncIterable<KeyValue> asyncIterable = transaction.getRange(
                                    KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, tickBackward(snapshotTo)))),
                                    KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, lowerBound))),
                                    PRIMARY_INDEX,
                                    ROW_LIMIT_UNLIMITED
                            );

                            return Flowable.fromPublisher(new AsyncIterablePublisher<>(asyncIterable))
                                    .map(kv -> toFragment(primary, kv, namespace, entity));
                        })
                );
    }

    @Override
    public Flowable<Fragment> readAll(Transaction tx, ZonedDateTime snapshot, String namespace, String entity, no.ssb.lds.api.persistence.reactivex.Range<String> range) {
        final OrderedKeyValueTransaction transaction = (OrderedKeyValueTransaction) tx;
        final AtomicReference<String> idRef = new AtomicReference<>();
        final AtomicReference<ZonedDateTime> versionRef = new AtomicReference<>();
        return Single.defer(() -> getPrimary(namespace, entity))
                .flatMapPublisher(primary -> Flowable.fromPublisher(
                        new AsyncIterablePublisher<>(transaction.getRange(
                                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(lexicoNext(ofNullable(range.getAfter()).orElse(" "))))),
                                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(lexicoPrev(ofNullable(range.getBefore()).orElse("~"))))),
                                PRIMARY_INDEX,
                                ROW_LIMIT_UNLIMITED
                        )))
                        .map(kv -> toFragment(primary, kv, namespace, entity))
                        .filter(fragment -> {
                            if (!fragment.id().equals(idRef.get())) {
                                idRef.set(fragment.id());
                                versionRef.set(null);
                            }
                            if (snapshot.isBefore(fragment.timestamp())) {
                                return false;
                            }
                            if (versionRef.get() == null) {
                                versionRef.set(fragment.timestamp());
                            }
                            return fragment.timestamp().equals(versionRef.get());
                        })
                );
    }

    @Override
    public Flowable<Fragment> find(Transaction tx, ZonedDateTime snapshot, String namespace, String
            entity, String path, byte[] value, no.ssb.lds.api.persistence.reactivex.Range<String> range) {
        return Single.defer(() -> getPrimary(namespace, entity)).flatMapPublisher(primary -> {
            String arrayIndexUnawarePath = Fragment.computeIndexUnawarePath(path, new ArrayList<>());
            return getIndex(namespace, entity, arrayIndexUnawarePath).flatMapPublisher(index -> {
                OrderedKeyValueTransaction transaction = (OrderedKeyValueTransaction) tx;
                byte[] truncatedValue = Fragment.hashOf(value);
                AsyncIterablePublisher<KeyValue> indexMatchesPublisher = new AsyncIterablePublisher<>(transaction.getRange(index.range(Tuple.from(truncatedValue)), PATH_VALUE_INDEX, ROW_LIMIT_UNLIMITED));
                AtomicReference<String> idRef = new AtomicReference<>(null);
                return Flowable.fromPublisher(indexMatchesPublisher).filter(pathValueIndexedKeyValue -> {
                    Tuple key = index.unpack(pathValueIndexedKeyValue.getKey());
                    Tuple matchedVersion = key.getNestedTuple(2);
                    if (toTimestamp(matchedVersion).isAfter(snapshot)) {
                        return false; // false-positive, version was not yet visible at snapshot time
                    }
                    String id = key.getString(1);
                    if (id.equals(idRef.get())) {
                        return false; // false-positive, older version of same resource as previous
                    }
                    idRef.set(id);
                    return true;
                }).concatMapEager(pathValueIndexedKeyValue -> {
                    Tuple key = index.unpack(pathValueIndexedKeyValue.getKey());
                    Tuple matchedVersion = key.getNestedTuple(2);
                    String id = key.getString(1);
                    return findAnyOneMatchingFragmentInPrimary(transaction, primary, id, toTuple(snapshot)).flatMapPublisher(aMatchingFragmentKv -> {
                        Tuple keyTuple = primary.unpack(aMatchingFragmentKv.getKey());
                        Tuple versionTuple = keyTuple.getNestedTuple(1);
                        FragmentType fragmentType = FragmentType.fromTypeCode(keyTuple.getBytes(3)[0]);
                        if (!matchedVersion.equals(versionTuple)) {
                            return Flowable.empty(); // false-positive index-match on older version
                        }
                        if (FragmentType.DELETED.equals(fragmentType)) {
                            // Version was overwritten in primary by a delete-marker, remove index fragment asynchronously in separate transaction.
                            transactionFactory().runAsyncInIsolatedTransaction(otherTx -> {
                                ((OrderedKeyValueTransaction) otherTx).clear(aMatchingFragmentKv.getKey(), PATH_VALUE_INDEX);
                                return null;
                            }, false);
                            return Flowable.empty();
                        }

                        // NOTE It's possible to get false-positive due to either index-value-truncation or value occupying
                        // multiple key-value slots. These must be discarded by client or the buffered persistence layer.

                        AsyncIterablePublisher<KeyValue> resourcePublisher = new AsyncIterablePublisher<>(transaction.getRange(primary.range(Tuple.from(id, matchedVersion)), PRIMARY_INDEX, ROW_LIMIT_UNLIMITED));
                        return Flowable.fromPublisher(resourcePublisher).map(kv -> toFragment(primary, kv, namespace, entity));
                    });
                });
            });
        });
    }

    @Override
    public Single<Boolean> hasPrevious(Transaction tx, ZonedDateTime snapshot, String namespace, String
            entityName, String id) {
        return readAll(tx, snapshot, namespace, entityName, no.ssb.lds.api.persistence.reactivex.Range.lastBefore(1, id)).isEmpty()
                .map(wasEmpty -> !wasEmpty);
    }

    @Override
    public Single<Boolean> hasNext(Transaction tx, ZonedDateTime snapshot, String namespace, String
            entityName, String id) {
        return readAll(tx, snapshot, namespace, entityName, no.ssb.lds.api.persistence.reactivex.Range.firstAfter(1, id)).isEmpty()
                .map(wasEmpty -> !wasEmpty);
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
    static Maybe<KeyValue> findAnyOneMatchingFragmentInPrimary(OrderedKeyValueTransaction transaction, Subspace
            primary, String id, Tuple snapshot) {
        /*
         * The range specified is guaranteed to never return more than 1 result. The returned KeyValue list will be one of:
         *   (1) Last fragment of matching resource when resource exists and client-timestamp is greater than or equal to resource timestamp
         *   (2) Last fragment of an unrelated resource when resource does not exist for the specified timestamp
         *   (3) KeyValue of another key-space than PRIMARY
         *   (4) Empty when there are no resources with the given id that are an older (or same) version than snapshot
         */

        AsyncIterable<KeyValue> version = transaction.getRange(
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(id, snapshot))),
                KeySelector.firstGreaterOrEqual(primary.pack(Tuple.from(lexicoNext(id)))),
                1,
                StreamingMode.EXACT,
                PRIMARY_INDEX
        );

        return Flowable.fromPublisher(new AsyncIterablePublisher<>(version)).firstElement().filter(kv -> {
            if (!primary.contains(kv.getKey())) {
                // (3) KeyValue of another key-space than PRIMARY
                return false;
            }

            Tuple keyTuple = primary.unpack(kv.getKey());
            String resourceId = keyTuple.getString(0);
            if (!id.equals(resourceId)) {
                // (2) fragment of an unrelated resource
                return false;
            }

            // (1) Match found
            return true;
        });
    }

    @Override
    public Completable delete(Transaction transaction, String namespace, String entity, String
            id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
        return doDelete((OrderedKeyValueTransaction) transaction, namespace, entity, id, toTuple(version), policy);
    }

    private Completable doDelete(OrderedKeyValueTransaction transaction, String namespace, String
            entity, String resourceId, Tuple resourceVersion, PersistenceDeletePolicy policy) {
        return Single.defer(() -> getPrimary(namespace, entity).doOnSuccess(primary -> {
            /*
             * Get all fragments of given versioned resource.
             */
            Range range = primary.range(Tuple.from(resourceId, resourceVersion));
            Flowable.fromPublisher(new AsyncIterablePublisher<>(transaction.getRange(range, PRIMARY_INDEX, ROW_LIMIT_UNLIMITED)))
                    .flatMapCompletable(kv -> deleteFragmentFromPathValueIndex(transaction, namespace, entity, primary, kv));
            transaction.clearRange(range, PRIMARY_INDEX);
        })).ignoreElement();
    }

    private Completable deleteFragmentFromPathValueIndex(OrderedKeyValueTransaction transaction, String namespace, String entity, Subspace primary, KeyValue kv) {
        Tuple key = primary.unpack(kv.getKey());
        String id = key.getString(0);
        Tuple version = key.getNestedTuple(1);
        String path = key.getString(2);
        long offset = key.getLong(4);

        if (offset > 0) {
            return Completable.complete();
        }

        ArrayList<Integer> indices = new ArrayList<>();
        String indexUnawarePath = Fragment.computeIndexUnawarePath(path, indices);
        Tuple arrayIndices = Tuple.from(indices);
        byte[] truncatedValue = Fragment.hashOf(kv.getValue());
        Tuple valueIndexKey = Tuple.from(
                truncatedValue,
                id,
                version,
                arrayIndices
        );

        return getIndex(namespace, entity, indexUnawarePath).doOnSuccess(index -> {
            byte[] binaryValueIndexKey = index.pack(valueIndexKey);
            transaction.clear(binaryValueIndexKey, PATH_VALUE_INDEX);
        }).ignoreElement();
    }

    @Override
    public Completable deleteAllVersions(Transaction transaction, String namespace, String
            entity, String id, PersistenceDeletePolicy policy) throws PersistenceException {
        return doDeleteAllVersions((OrderedKeyValueTransaction) transaction, namespace, entity, id, policy);
    }

    private Completable doDeleteAllVersions(OrderedKeyValueTransaction transaction, String
            namespace, String entity, String resourceId, PersistenceDeletePolicy policy) {
        return Completable.defer(() -> getPrimary(namespace, entity)
                .flatMapCompletable(primary -> Flowable.fromPublisher(new AsyncIterablePublisher<>(transaction.getRange(primary.range(Tuple.from(resourceId)), PRIMARY_INDEX, ROW_LIMIT_UNLIMITED)))
                        .flatMapCompletable(kv -> deleteFragmentFromPathValueIndex(transaction, namespace, entity, primary, kv))
                        .doOnComplete(() -> transaction.clearRange(primary.range(Tuple.from(resourceId)), PRIMARY_INDEX))));
    }

    @Override
    public Completable deleteAllEntities(Transaction transaction, String namespace, String entity, Iterable<String> paths) {
        return doDeleteAllEntities((OrderedKeyValueTransaction) transaction, namespace, entity, paths);
    }

    private Completable doDeleteAllEntities(OrderedKeyValueTransaction transaction, String namespace, String entity, Iterable<String> paths) {
        return Completable.merge(List.of(
                Completable.defer(() -> getPrimary(namespace, entity).flatMapCompletable(primary -> {
                    transaction.clearRange(primary.range(), PRIMARY_INDEX);
                    return Completable.complete();
                })),
                Flowable.fromIterable(paths).flatMapCompletable(path -> getIndex(namespace, entity, path).flatMapCompletable(index -> {
                    transaction.clearRange(index.range(), PATH_VALUE_INDEX);
                    return Completable.complete();
                }))
        ));
    }

    @Override
    public Completable markDeleted(Transaction transaction, String namespace, String entity, String
            id, ZonedDateTime version, PersistenceDeletePolicy policy) throws PersistenceException {
        return doMarkDeleted((OrderedKeyValueTransaction) transaction, toTuple(version), namespace, entity, id, policy);
    }

    Completable doMarkDeleted(OrderedKeyValueTransaction transaction, Tuple version, String
            namespace, String entity, String id, PersistenceDeletePolicy policy) {
        return getPrimary(namespace, entity).doOnSuccess(primary -> {
            // Clear primary of existing document with same version
            transaction.clearRange(primary.range(Tuple.from(id, version)), PRIMARY_INDEX);

            /*
             * Insert delete marker in PRIMARY
             */
            Tuple primaryKey = Tuple.from(
                    id,
                    version,
                    "",
                    new byte[]{FragmentType.DELETED.getTypeCode()},
                    0
            );
            byte[] binaryPrimaryKey = primary.pack(primaryKey);
            transaction.set(binaryPrimaryKey, EMPTY_BYTE_ARRAY, PRIMARY_INDEX);
        }).ignoreElement();
    }

    @Override
    public void close() throws PersistenceException {
        transactionFactory.close();
    }

    public static Tuple toTuple(ZonedDateTime timestamp) {
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

    public static ZonedDateTime toTimestamp(Tuple timestampTuple) {
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

    static String lexicoNext(String lexico) {
        return lexico + " ";
    }

    static String lexicoPrev(String lexico) {
        return lexico.substring(0, lexico.length() - 1) + (char) (lexico.charAt(lexico.length() - 1) - 1) + "~";
    }

}
