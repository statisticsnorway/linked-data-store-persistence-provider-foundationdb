package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.PersistenceException;

import java.util.concurrent.CompletableFuture;

public interface FoundationDBDirectory {

    CompletableFuture<? extends Subspace> createOrOpen(Tuple key) throws PersistenceException;
}
