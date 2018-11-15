package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import no.ssb.lds.api.persistence.PersistenceResult;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class FoundationDBSubscription implements Flow.Subscription {
    final Database db;
    final AtomicLong budget = new AtomicLong(0);
    final AtomicBoolean first = new AtomicBoolean(true);
    final AtomicReference<Transaction> transactionRef = new AtomicReference<>();
    final Flow.Subscriber<? super PersistenceResult> subscriber;
    final AtomicReference<Consumer<Long>> firstRequestCallback = new AtomicReference<>();
    final AtomicReference<Consumer<Long>> requestCallback = new AtomicReference<>();
    final AtomicReference<Consumer<Void>> cancelCallback = new AtomicReference<>();

    FoundationDBSubscription(Database db, Flow.Subscriber<? super PersistenceResult> subscriber) {
        this.db = db;
        this.subscriber = subscriber;
    }

    void registerFirstRequest(Consumer<Long> callback) {
        firstRequestCallback.set(callback);
    }

    void registerRequest(Consumer<Long> callback) {
        requestCallback.set(callback);
    }

    void registerCancel(Consumer<Void> callback) {
        cancelCallback.set(callback);
    }

    @Override
    public void request(long n) {
        try {
            if (budget.getAndAdd(n) > 0) {
                // back-pressure arrived before budget was exhausted
                return;
            }
            if (first.compareAndSet(true, false)) {
                transactionRef.set(db.createTransaction());
                firstRequestCallback.get().accept(n);
            }
            if (requestCallback.get() != null) {
                requestCallback.get().accept(n);
            }
        } catch (Throwable t) {
            subscriber.onError(t);
        }
    }

    @Override
    public void cancel() {
        try {
            if (cancelCallback.get() != null) {
                cancelCallback.get().accept(null);
            }
        } finally {
            if (transactionRef.get() != null) {
                transactionRef.get().cancel();
            }
        }
    }

    void onError(Throwable t) {
        try {
            transactionRef.get().cancel();
            transactionRef.get().close();
        } finally {
            subscriber.onError(t);
        }
    }

    void onNext(PersistenceResult persistenceResult) {
        subscriber.onNext(persistenceResult);
    }

    public void onComplete() {
        subscriber.onComplete();
    }
}