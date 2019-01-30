package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class AsyncIterablePublisher<T> implements Publisher<T> {

    final AsyncIterator<T> asyncIterator;

    public AsyncIterablePublisher(AsyncIterable<T> asyncIterable) {
        this.asyncIterator = asyncIterable.iterator();
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        subscriber.onSubscribe(new AsyncIterableSubscription(subscriber));
        if (asyncIterator == null) {
            subscriber.onError(new NullPointerException("asyncIterator"));
        }
    }

    class AsyncIterableSubscription implements Subscription {
        final AtomicLong requested = new AtomicLong();
        final AtomicLong published = new AtomicLong();
        final AtomicBoolean cancelled = new AtomicBoolean();
        final AtomicReference<Subscriber<? super T>> subscriber = new AtomicReference<>();
        final AtomicBoolean publicationPending = new AtomicBoolean();

        AsyncIterableSubscription(Subscriber<? super T> subscriber) {
            this.subscriber.set(subscriber);
        }

        @Override
        public void request(long n) {
            try {
                if (n <= 0) {
                    throw new IllegalArgumentException("requested amount must be > 0");
                }
                if (cancelled.get()) {
                    return;
                }
                requested.addAndGet(n);
                queuePublicationRequest(() -> asyncIterate());
            } catch (Throwable t) {
                Subscriber<? super T> subscriber = this.subscriber.get();
                if (subscriber == null) {
                    // asynchronously cancelled
                    return;
                }
                subscriber.onError(t);
            }
        }

        private void asyncIterate() {
            asyncIterator.onHasNext().thenAccept(hasNext -> {
                Subscriber<? super T> subscriber = this.subscriber.get();
                if (subscriber == null) {
                    // asynchronously cancelled
                    return;
                }
                if (hasNext) {
                    try {
                        T item = asyncIterator.next();
                        subscriber.onNext(item);
                        published.incrementAndGet();
                    } catch (Throwable t) {
                        subscriber.onError(t);
                    } finally {
                        publicationPending.set(false);
                    }
                    queuePublicationRequest(() -> asyncIterate());
                } else {
                    subscriber.onComplete();
                }
            });
        }

        @Override
        public void cancel() {
            cancelled.set(true);
            subscriber.set(null);
        }

        public void queuePublicationRequest(Runnable runnable) {
            if (cancelled.get()) {
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
}