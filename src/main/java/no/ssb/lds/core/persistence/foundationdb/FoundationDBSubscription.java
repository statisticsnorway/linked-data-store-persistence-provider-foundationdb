package no.ssb.lds.core.persistence.foundationdb;

import no.ssb.lds.api.persistence.Fragment;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class FoundationDBSubscription implements Flow.Subscription {
    final AtomicLong budget = new AtomicLong(0);
    final AtomicBoolean first = new AtomicBoolean(true);
    final Flow.Subscriber<? super Fragment> subscriber;
    final AtomicReference<Consumer<Long>> firstRequestCallback = new AtomicReference<>();
    final AtomicReference<Consumer<Long>> requestOnBudgetPositiveCallback = new AtomicReference<>();
    final AtomicReference<Consumer<Void>> cancelCallback = new AtomicReference<>();

    FoundationDBSubscription(Flow.Subscriber<? super Fragment> subscriber) {
        this.subscriber = subscriber;
    }

    FoundationDBSubscription registerFirstRequest(Consumer<Long> callback) {
        firstRequestCallback.set(callback);
        return this;
    }

    FoundationDBSubscription registerOnBudgetPositive(Consumer<Long> callback) {
        requestOnBudgetPositiveCallback.set(callback);
        return this;
    }

    FoundationDBSubscription registerCancel(Consumer<Void> callback) {
        cancelCallback.set(callback);
        return this;
    }

    @Override
    public void request(long n) {
        try {
            if (budget.getAndAdd(n) > 0) {
                // back-pressure arrived before budget was exhausted
                return;
            }
            // budget was exhausted before back-pressure arrived
            if (first.compareAndSet(true, false)) {
                // first request
                firstRequestCallback.get().accept(n);
            } else {
                // not-first requests
                if (requestOnBudgetPositiveCallback.get() != null) {
                    requestOnBudgetPositiveCallback.get().accept(n);
                }
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