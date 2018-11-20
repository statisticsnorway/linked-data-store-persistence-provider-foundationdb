package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.Fragment;

import java.util.Collections;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.EMPTY_BYTE_ARRAY;
import static no.ssb.lds.core.persistence.foundationdb.FoundationDBPersistence.toTimestamp;

class PrimaryIterator implements Consumer<Boolean> {

    final FoundationDBSubscription subscription;
    final Tuple snapshot;
    final FoundationDBTransaction transaction;
    final String namespace;
    final String entity;
    final DirectorySubspace primary;
    final AsyncIterator<KeyValue> iterator;
    final NavigableSet<String> remainingIds;
    final int limit;

    final AtomicBoolean cancel = new AtomicBoolean(false);

    final Set<String> processingThreads = new ConcurrentSkipListSet<>();

    final AtomicReference<Fragment> fragmentToPublish = new AtomicReference<>();
    final AtomicInteger fragmentsPublished = new AtomicInteger(0);

    final AtomicReference<String> fragmentsId = new AtomicReference<>();
    final AtomicReference<Tuple> fragmentsVersion = new AtomicReference<>();

    // signalled with number of fragments published by this iterator
    final CompletableFuture<Integer> doneSignal = new CompletableFuture<>();

    PrimaryIterator(FoundationDBSubscription subscription, Tuple snapshot, FoundationDBTransaction transaction, String namespace, String entity, NavigableSet<String> ids, DirectorySubspace primary, AsyncIterator<KeyValue> iterator, int limit) {
        this.subscription = subscription;
        this.snapshot = snapshot;
        this.transaction = transaction;
        this.namespace = namespace;
        this.entity = entity;
        this.primary = primary;
        this.iterator = iterator;
        if (ids == null) {
            this.remainingIds = null;
        } else {
            this.remainingIds = Collections.synchronizedNavigableSet(new TreeSet<>(ids));
        }
        this.limit = limit;
        subscription.registerCancel(v -> cancel.set(true));
        subscription.registerOnBudgetPositive(n -> applyBackpressure(n));
        checkAndRecordProcessingThread();
    }

    private void checkAndRecordProcessingThread() {
        String name = Thread.currentThread().getName();
        if (!processingThreads.contains(name)) {
            processingThreads.add(name);
            System.out.format("%s :: %s :: accept()%n", Thread.currentThread().getName(), this.toString());
        }
    }

    void applyBackpressure(long n) {
        // budget was exhausted
        iterator.onHasNext().thenAccept(this);
    }

    @Override
    public void accept(Boolean hasNext) {
        try {
            checkAndRecordProcessingThread();
            if (hasNext) {
                onAsyncIteratorHasNext();
            } else {
                onAsyncIteratorHasNoMore();
            }
        } catch (Throwable t) {
            iterator.cancel();
            doneSignal.completeExceptionally(t);
        }
    }

    void onAsyncIteratorHasNext() {
        KeyValue kv = iterator.next();

        returnStolenBudget();

        if (cancel.get()) {
            // honor external cancel signal
            iterator.cancel();
            signalComplete();
            return;
        }

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

        if (fragmentsId.get() == null) {
            this.fragmentsId.set(dbId);
        }

        if (!dbId.equals(fragmentsId.get())) {
            if (remainingIds != null) {
                remainingIds.remove(fragmentsId.get());
            }
            fragmentsId.set(dbId);
            fragmentsVersion.set(null);
        }

        if (snapshot != null) {
            if (snapshot.compareTo(version) > 0) {
                // ignore versions newer than snapshot
                iterator.onHasNext().thenAccept(this);
                return;
            }

            if (!fragmentsVersion.compareAndSet(null, version)) {
                if (!version.equals(fragmentsVersion.get())) {
                    // already matched a more recent version closer to snapshot
                    iterator.onHasNext().thenAccept(this);
                    return;
                }
            }
        }

        if (fragmentsPublished.get() >= limit) {
            // reached limit and there are more matching fragments.
            subscription.onNext(new Fragment(true, Fragment.LIMITED_CODE, namespace, entity, dbId, toTimestamp(version), path, offset, EMPTY_BYTE_ARRAY));
            signalComplete();
            return;
        }

        if (!fragmentToPublish.compareAndSet(null, new Fragment(namespace, entity, dbId, toTimestamp(version), path, offset, kv.getValue()))) {
            throw new IllegalStateException("Previous fragment was not published!");
        }

        if (subscription.budget.getAndDecrement() <= 0) {
            // budget stolen, will be returned when more back-pressure is applied.
            return;
        }

        publishFragment();

        iterator.onHasNext().thenAccept(this);
    }

    private void publishFragment() {
        Fragment fragment = fragmentToPublish.getAndSet(null);
        if (fragment != null) {
            subscription.onNext(fragment);
            fragmentsPublished.incrementAndGet();
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
        doneSignal.complete(fragmentsPublished.get());
    }
}