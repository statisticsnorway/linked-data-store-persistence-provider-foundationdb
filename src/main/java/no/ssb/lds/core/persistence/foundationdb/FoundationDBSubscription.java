package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.Database;
import no.ssb.lds.api.persistence.Fragment;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class FoundationDBSubscription implements Flow.Subscription {
    final Database db;
    final AtomicLong budget = new AtomicLong(0);
    final AtomicBoolean first = new AtomicBoolean(true);
    final Flow.Subscriber<? super Fragment> subscriber;
    final AtomicReference<Consumer<Long>> firstRequestCallback = new AtomicReference<>();
    final AtomicReference<Consumer<Long>> requestCallback = new AtomicReference<>();
    final AtomicReference<Consumer<Void>> cancelCallback = new AtomicReference<>();

    FoundationDBSubscription(Database db, Flow.Subscriber<? super Fragment> subscriber) {
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
        if (cancelCallback.get() != null) {
            cancelCallback.get().accept(null);
        }
    }

    void onError(Throwable t) {
        subscriber.onError(t);
    }

    void onNext(Fragment fragment) {
        subscriber.onNext(fragment);
    }

    public void onComplete() {
        subscriber.onComplete();
    }
}