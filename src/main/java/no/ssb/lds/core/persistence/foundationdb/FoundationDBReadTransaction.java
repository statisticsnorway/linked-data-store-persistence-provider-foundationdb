package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.subspace.Subspace;
import no.ssb.lds.api.persistence.TransactionStatistics;

import java.util.concurrent.CompletableFuture;

public class FoundationDBReadTransaction implements OrderedKeyValueTransaction {

    protected final com.apple.foundationdb.ReadTransaction fdbReadTransaction;
    protected final FoundationDBStatistics statistics = new FoundationDBStatistics();

    FoundationDBReadTransaction(com.apple.foundationdb.ReadTransaction fdbReadTransaction) {
        this.fdbReadTransaction = fdbReadTransaction;
    }

    @Override
    public CompletableFuture<TransactionStatistics> commit() {
        return CompletableFuture.completedFuture(statistics);
    }

    @Override
    public CompletableFuture<TransactionStatistics> cancel() {
        return CompletableFuture.completedFuture(statistics);
    }

    @Override
    public void clearRange(Range range, String index) {
        throw new UnsupportedOperationException("clear range not allowed in read-only transaction");
    }

    @Override
    public void set(byte[] key, byte[] value, String index) {
        throw new UnsupportedOperationException("set key not allowed in read-only transaction");

    }

    @Override
    public void clear(byte[] key, String index) {
        throw new UnsupportedOperationException("clear key not allowed in read-only transaction");
    }

    public AsyncIterable<KeyValue> getRange(Range range, String index) {
        AsyncIterable<KeyValue> iterable = fdbReadTransaction.getRange(range);
        statistics.getRange(index);
        return iterable;
    }

    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, String index) {
        AsyncIterable<KeyValue> iterable = fdbReadTransaction.getRange(begin, end);
        statistics.getRange(index);
        return iterable;
    }

    public AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, StreamingMode streamingMode, String index) {
        AsyncIterable<KeyValue> iterable = fdbReadTransaction.getRange(begin, end, limit, false, streamingMode);
        statistics.getRange(index);
        return iterable;
    }

    @Override
    public void dumpIndex(String index, Subspace subspace) {
        // this is used for debugging only by in-memory test module
        throw new UnsupportedOperationException();
    }
}
