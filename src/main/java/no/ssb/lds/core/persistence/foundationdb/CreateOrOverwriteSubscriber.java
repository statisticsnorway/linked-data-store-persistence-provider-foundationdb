package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.PersistenceStatistics;

import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.EMPTY_BYTE_ARRAY;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.MAX_DESIRED_KEY_LENGTH;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PATH_VALUE_INDEX;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PRIMARY_INDEX;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.toTuple;

public class CreateOrOverwriteSubscriber implements Flow.Subscriber<Fragment> {

    final FoundationDBPersistence persistence;
    final CompletableFuture<PersistenceStatistics> result;
    final FoundationDBStatistics statistics;

    final AtomicReference<Flow.Subscription> subscriptionRef = new AtomicReference<>();
    final AtomicReference<Transaction> transactionRef = new AtomicReference<>();
    final AtomicReference<DirectorySubspace> primaryRef = new AtomicReference<>();
    final Map<Tuple, DirectorySubspace> indexByTuple = new ConcurrentHashMap<>();
    final CopyOnWriteArraySet<Range> clearedRanges = new CopyOnWriteArraySet<>();

    final CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
    final AtomicReference<Fragment> previousFragmentRef = new AtomicReference<>();

    public CreateOrOverwriteSubscriber(FoundationDBPersistence persistence, CompletableFuture<PersistenceStatistics> result, FoundationDBStatistics statistics) {
        this.persistence = persistence;
        this.result = result;
        this.statistics = statistics;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscriptionRef.set(subscription);
        subscription.request(1);
        transactionRef.set(persistence.db.createTransaction());
    }

    @Override
    public void onNext(Fragment fragment) {
        try {
            DirectorySubspace primary = primaryRef.get();
            if (primary == null) {
                persistence.getPrimary(fragment.namespace(), fragment.entity()).thenAccept(p -> {
                    primaryRef.set(p);
                    onNext(fragment);
                });
                return;
            }

            ArrayList<Integer> arrayIndices = new ArrayList<>();
            String indexUnawarePath = Fragment.computeIndexUnawarePath(fragment.path(), arrayIndices);

            Tuple indexTuple = Tuple.from(fragment.namespace(), fragment.entity(), indexUnawarePath);
            DirectorySubspace index = indexByTuple.get(indexTuple);
            if (index == null) {
                persistence.getIndex(fragment.namespace(), fragment.entity(), indexUnawarePath).thenAccept(i -> {
                    indexByTuple.put(indexTuple, i);
                    onNext(fragment);
                });
                return;
            }

            try {

                Tuple fragmentVersion = toTuple(fragment.timestamp());

                // Clear primary of existing document with same version
                Range range = primary.range(Tuple.from(fragment.id(), fragmentVersion));
                if (!clearedRanges.contains(range)) {
                    clearedRanges.add(range);
                    transactionRef.get().clear(range);
                    statistics.clearRange(PRIMARY_INDEX);
                }

                // NOTE: With current implementation we do not need to clear the index. False-positive matches in the index
                // are always followed up by a primary lookup. Clearing Index space is expensive as it requires a read to
                // figure out whether there is anything to clear and then another read to get existing doument and then finally
                // clearing each document fragment independently from the existing document in the index space which cannot be
                // done with a single range operation and therefore must be done using individual write operations per fragment.

                /*
                 * PRIMARY
                 */

                if (!fragment.samePathAs(previousFragmentRef.get())) {
                    encoder.reset();
                }

                Tuple primaryKey = Tuple.from(
                        fragment.id(),
                        fragmentVersion,
                        fragment.path(),
                        fragment.offset()
                );
                byte[] binaryPrimaryKey = primary.pack(primaryKey);

                transactionRef.get().set(binaryPrimaryKey, fragment.value());
                statistics.setKeyValue(PRIMARY_INDEX);

                if (fragment.offset() == 0) {
                    /*
                     * INDEX
                     */
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
                    transactionRef.get().set(binaryValueIndexKey, EMPTY_BYTE_ARRAY);
                    statistics.setKeyValue(PATH_VALUE_INDEX);
                }

                subscriptionRef.get().request(1);

            } finally {
                previousFragmentRef.set(fragment);

            }
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        result.completeExceptionally(throwable);
    }

    @Override
    public void onComplete() {
        try {
            transactionRef.get().commit();
            result.complete(statistics);
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
    }
}
