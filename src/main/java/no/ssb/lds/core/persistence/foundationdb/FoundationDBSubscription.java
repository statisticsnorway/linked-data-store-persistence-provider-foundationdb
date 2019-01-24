package no.ssb.lds.core.persistence.foundationdb;

import no.ssb.lds.api.persistence.streaming.Fragment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class FoundationDBSubscription implements Flow.Subscription {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBSubscription.class);

    private final AtomicLong requested = new AtomicLong(0);
    private final AtomicLong published = new AtomicLong(0);
    private final AtomicBoolean publicationPending = new AtomicBoolean(false);
    private final AtomicReference<Flow.Subscriber<? super Fragment>> subscriber = new AtomicReference<>();
    private final AtomicReference<Runnable> firstRequestCallback = new AtomicReference<>();
    private final AtomicReference<Runnable> requestOnBudgetPositiveCallback = new AtomicReference<>();
    private final AtomicReference<Runnable> cancelCallback = new AtomicReference<>();

    // Used to synchronize all onX calls to subscriber.
    private final Object sync = new Object();

    FoundationDBSubscription(Flow.Subscriber<? super Fragment> subscriber) {
        subscriber.getClass(); // reactive-streams spec 1.09: force NPE from JVM if subscriber is null
        this.subscriber.set(subscriber);
    }

    FoundationDBSubscription registerFirstRequest(Runnable callback) {
        firstRequestCallback.set(callback);
        return this;
    }

    FoundationDBSubscription registerOnBudgetPositive(Runnable callback) {
        requestOnBudgetPositiveCallback.set(callback);
        return this;
    }

    FoundationDBSubscription registerCancel(Runnable callback) {
        cancelCallback.set(callback);
        return this;
    }

    public void onSubscribe() {
        Flow.Subscriber<? super Fragment> subscriber = this.subscriber.get();
        if (subscriber != null) {
            synchronized (sync) {
                subscriber.onSubscribe(this);
            }
        }
    }

    @Override
    public void request(long n) {
        try {
            if (n <= 0) {
                throw new IllegalArgumentException("requested amount must be > 0");
            }
            if (isCancelled()) {
                return;
            }
            long r = this.requested.getAndAdd(n);
            if (r == 0) {
                // first request
                firstRequestCallback.get().run();
            } else {
                // not-first requests
                if (requestOnBudgetPositiveCallback.get() != null) {
                    requestOnBudgetPositiveCallback.get().run();
                }
            }
        } catch (Throwable t) {
            onError(t);
        }
    }

    @Override
    public void cancel() {
        subscriber.set(null);
        if (cancelCallback.get() != null) {
            cancelCallback.get().run();
        }
    }

    public boolean isCancelled() {
        return subscriber.get() == null;
    }

    void onError(Throwable t) {
        Flow.Subscriber<? super Fragment> subscriber = this.subscriber.get();
        if (subscriber != null) {
            synchronized (sync) {
                subscriber.onError(t);
            }
        } else {
            log.warn("subscription was cancelled, unable to produce onError signal", t);
        }
    }

    final AtomicInteger count = new AtomicInteger();

    void onNext(Fragment fragment) {
        try {
            int countAfter = count.incrementAndGet();
            if (countAfter > 1) {
                log.warn("More than one thread publishing onNext(Fragment) at the same time!");
            }
            Flow.Subscriber<? super Fragment> subscriber = this.subscriber.get();
            if (subscriber != null) {
                synchronized (sync) {
                    subscriber.onNext(fragment);
                }
            }
            published.incrementAndGet();
        } finally {
            count.decrementAndGet();
            publicationPending.set(false);
        }
    }

    public void onComplete() {
        Flow.Subscriber<? super Fragment> subscriber = this.subscriber.get();
        if (subscriber != null) {
            synchronized (sync) {
                subscriber.onComplete();
            }
        }
    }

    public void queuePublicationRequest(Runnable runnable) {
        if (isCancelled()) {
            return;
        }
        // loop until stable. Instability is caused by concurrency
        for (; ; ) {
            if (!publicationPending.compareAndSet(false, true)) {
                return; // a publication is already pending
            }
            long p = published.get();
            long r = requested.get();
            if (r > p) {
                // available budget
                runnable.run(); // runnable should eventually cause onNext publication
                return;
            } else {
                publicationPending.set(false);
                // re-check whether requested count has increased while publicationPending flag was set
                if (r == requested.get()) {
                    // requested count has not changed
                    return;
                }
            }
        }
    }
}