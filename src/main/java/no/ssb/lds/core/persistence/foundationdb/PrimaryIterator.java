package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.PersistenceResult;

import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.PRIMARY_INDEX;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.toTimestamp;

class PrimaryIterator implements Consumer<Boolean> {

    final FoundationDBSubscription subscription;
    final Tuple snapshot;
    final FoundationDBStatistics statistics;
    final String namespace;
    final String entity;
    final DirectorySubspace primary;
    final AsyncIterator<KeyValue> iterator;
    final NavigableSet<String> remainingIds;
    final int limit;

    final AtomicBoolean cancel = new AtomicBoolean(false);

    Fragment fragmentToPublish;
    int fragmentsPublished = 0;

    String fragmentsId;
    Tuple fragmentsVersion;

    // signalled with number of fragments published by this iterator
    final CompletableFuture<Integer> doneSignal = new CompletableFuture<>();

    PrimaryIterator(FoundationDBSubscription subscription, Tuple snapshot, FoundationDBStatistics statistics, String namespace, String entity, NavigableSet<String> ids, DirectorySubspace primary, AsyncIterator<KeyValue> iterator, int limit) {
        this.subscription = subscription;
        this.snapshot = snapshot;
        this.statistics = statistics;
        this.namespace = namespace;
        this.entity = entity;
        this.primary = primary;
        this.iterator = iterator;
        if (ids == null) {
            this.remainingIds = null;
        } else {
            this.remainingIds = new TreeSet<>(ids);
        }
        this.limit = limit;
        subscription.registerCancel(v -> cancel.set(true));
        subscription.registerRequest(n -> applyBackpressure(n));
    }

    void applyBackpressure(long n) {
        // budget was exhausted
        iterator.onHasNext().thenAccept(this);
    }

    @Override
    public void accept(Boolean hasNext) {
        try {
            if (hasNext) {
                onAsyncIteratorHasNext();
            } else {
                onAsyncIteratorHasNoMore();
            }
        } catch (Throwable t) {
            iterator.cancel();
            subscription.onError(t);
        }
    }

    void onAsyncIteratorHasNext() {
        returnStolenBudget();

        if (cancel.get()) {
            // honor external cancel signal
            iterator.cancel();
            signalComplete();
            return;
        }

        KeyValue kv = iterator.next();
        statistics.rangeIteratorNext(PRIMARY_INDEX);

        Tuple keyTuple = primary.unpack(kv.getKey());
        String dbId = keyTuple.getString(0);
        Tuple version = keyTuple.getNestedTuple(1);
        String path = keyTuple.getString(2);
        long offset = keyTuple.getLong(3);

        if (remainingIds != null) {
            if (remainingIds.isEmpty()) {
                // no more resources wanted
                iterator.cancel();
                signalComplete();
                return;
            }
            if (dbId.compareTo(remainingIds.last()) > 0) {
                // no more resources in iterator match wanted resources
                iterator.cancel();
                signalComplete();
                return;
            }
            if (!remainingIds.contains(dbId)) {
                // resource not wanted
                iterator.onHasNext().thenAccept(this);
                return;
            }
        }

        if (fragmentsId == null) {
            fragmentsId = dbId;
        }

        if (!dbId.equals(fragmentsId)) {
            if (remainingIds != null) {
                remainingIds.remove(fragmentsId);
            }
            fragmentsId = dbId;
            fragmentsVersion = null;
        }

        if (snapshot != null && snapshot.compareTo(version) > 0) {
            // ignore versions newer than snapshot
            iterator.onHasNext().thenAccept(this);
            return;
        }

        if (fragmentsVersion == null) {
            // first fragment matching correct version of resource
            fragmentsVersion = version;
        }

        if (!version.equals(fragmentsVersion)) {
            // already matched a more recent version closer to snapshot
            iterator.onHasNext().thenAccept(this);
            return;
        }

        if (fragmentsPublished >= limit) {
            // reached limit and there are more matching fragments.
            subscription.onNext(new PersistenceResult(Fragment.DONE, statistics, true));
            signalComplete();
            return;
        }

        fragmentToPublish = new Fragment(namespace, entity, dbId, toTimestamp(version), path, offset, kv.getValue());

        if (subscription.budget.getAndDecrement() <= 0) {
            // budget stolen, will be returned when more back-pressure is applied.
            return;
        }

        publishFragment();

        iterator.onHasNext().thenAccept(this);
    }

    private void publishFragment() {
        if (fragmentToPublish != null) {
            subscription.onNext(new PersistenceResult(fragmentToPublish, statistics, false));
            fragmentToPublish = null;
            fragmentsPublished++;
        }
    }

    void returnStolenBudget() {
        publishFragment();
    }

    void onAsyncIteratorHasNoMore() {
        returnStolenBudget();
        signalComplete();
    }

    void signalComplete() {
        subscription.onComplete();
        doneSignal.complete(fragmentsPublished);
    }
}