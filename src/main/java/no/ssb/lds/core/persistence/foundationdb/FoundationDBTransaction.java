package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.async.AsyncIterable;
import no.ssb.lds.api.persistence.TransactionStatistics;

import java.util.concurrent.CompletableFuture;

public class FoundationDBTransaction implements OrderedKeyValueTransaction {

    private final com.apple.foundationdb.Transaction fdbTrans;
    private final FoundationDBStatistics statistics = new FoundationDBStatistics();

    FoundationDBTransaction(com.apple.foundationdb.Transaction fdbTrans) {
        this.fdbTrans = fdbTrans;
    }

    @Override
    public CompletableFuture<TransactionStatistics> commit() {
        return fdbTrans.commit().thenApply(v -> statistics);
    }

    @Override
    public CompletableFuture<TransactionStatistics> cancel() {
        fdbTrans.cancel();
        return CompletableFuture.completedFuture(statistics);
    }

    @Override
    public void close() {
        OrderedKeyValueTransaction.super.close();
        fdbTrans.close();
    }

    public void clearRange(Range range, String index) {
        fdbTrans.clear(range);
        statistics.clearRange(index);
    }

    public void set(byte[] key, byte[] value, String index) {
        fdbTrans.set(key, value);
        statistics.setKeyValue(index);
    }

    public AsyncIterable<KeyValue> getRange(Range range, String index) {
        AsyncIterable<KeyValue> iterable = fdbTrans.getRange(range);
        statistics.getRange(index);
        return iterable;
    }

    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, String index) {
        AsyncIterable<KeyValue> iterable = fdbTrans.getRange(begin, end);
        statistics.getRange(index);
        return iterable;
    }

    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, StreamingMode streamingMode, String index) {
        AsyncIterable<KeyValue> iterable = fdbTrans.getRange(begin, end, limit, false, streamingMode);
        statistics.getRange(index);
        return iterable;
    }

    public void clear(byte[] key, String index) {
        fdbTrans.clear(key);
        statistics.clearKeyValue(index);
    }
}
