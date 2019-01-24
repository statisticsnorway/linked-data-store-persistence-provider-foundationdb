package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.Range;
import no.ssb.lds.api.persistence.TransactionStatistics;

import java.util.concurrent.CompletableFuture;

public class FoundationDBTransaction extends FoundationDBReadTransaction {

    private final com.apple.foundationdb.Transaction fdbTransaction;

    FoundationDBTransaction(com.apple.foundationdb.Transaction fdbTransaction) {
        super(fdbTransaction);
        this.fdbTransaction = fdbTransaction;
    }

    @Override
    public CompletableFuture<TransactionStatistics> commit() {
        return fdbTransaction.commit().thenApply(v -> statistics);
    }

    @Override
    public CompletableFuture<TransactionStatistics> cancel() {
        fdbTransaction.cancel();
        return CompletableFuture.completedFuture(statistics);
    }

    @Override
    public void close() {
        super.close();
        fdbTransaction.close();
    }

    public void clearRange(Range range, String index) {
        fdbTransaction.clear(range);
        statistics.clearRange(index);
    }

    public void set(byte[] key, byte[] value, String index) {
        fdbTransaction.set(key, value);
        statistics.setKeyValue(index);
    }

    public void clear(byte[] key, String index) {
        fdbTransaction.clear(key);
        statistics.clearKeyValue(index);
    }
}
