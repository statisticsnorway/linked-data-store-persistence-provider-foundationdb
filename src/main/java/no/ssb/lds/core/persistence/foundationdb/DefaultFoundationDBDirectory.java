package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultFoundationDBDirectory implements FoundationDBDirectory {

    final Database db;
    final Directory directory;
    final Map<Tuple, CompletableFuture<DirectorySubspace>> directorySubspaceByPaths = new ConcurrentHashMap<>();

    public DefaultFoundationDBDirectory(Database db, Directory directory) {
        this.db = db;
        this.directory = directory;
    }

    @Override
    public CompletableFuture<? extends Subspace> createOrOpen(Tuple key) {
        // To create a nested subdirectory per tuple item, use: directory.createOrOpen(db, t.stream().map(o -> (String) o).collect(Collectors.toList())
        return directorySubspaceByPaths.computeIfAbsent(key, k -> directory.createOrOpen(db, List.of(k.toString())));
    }
}
