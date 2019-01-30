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
    public <T> CompletableFuture<T> runAsyncInIsolatedTransaction(Function<? super Transaction, ? extends T> retryable, boolean readOnly) {
        CompletableFuture<T> future = new CompletableFuture<>();
        if (readOnly) {
            db.readAsync(tx -> CompletableFuture.completedFuture(retryable.apply(new FoundationDBReadTransaction(tx))).thenAccept(t -> future.complete(t)));
        } else {
            db.runAsync(tx -> CompletableFuture.completedFuture(retryable.apply(new FoundationDBTransaction(tx))).thenAccept(t -> future.complete(t)));
        }
        return future;
    }

    @Override
    public Transaction createTransaction(boolean readOnly) {
        if (readOnly) {
            return new FoundationDBReadTransaction(db.createTransaction().snapshot());
        } else {
            return new FoundationDBTransaction(db.createTransaction());
        }
    }

    @Override
    public void close() {
        db.close();
    }
}
