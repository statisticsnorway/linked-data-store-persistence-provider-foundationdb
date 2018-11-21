package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.Database;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.TransactionFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class FoundationDBTransactionFactory implements TransactionFactory {

    final Database db;

    FoundationDBTransactionFactory(Database db) {
        this.db = db;
    }

    @Override
    public <T> CompletableFuture<T> runAsyncInIsolatedTransaction(Function<? super Transaction, ? extends CompletableFuture<T>> retryable) {
        return db.runAsync(tx -> retryable.apply(new FoundationDBTransaction(tx)));
    }

    @Override
    public FoundationDBTransaction createTransaction(boolean readOnly) {
        return new FoundationDBTransaction(db.createTransaction());
    }

    @Override
    public void close() {
        db.close();
    }
}
