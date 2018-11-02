package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import no.ssb.lds.api.persistence.PersistenceInitializer;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class FoundationDBInitializer implements PersistenceInitializer {
    @Override
    public String persistenceProviderId() {
        return "foundationdb";
    }

    @Override
    public Set<String> configurationKeys() {
        return Collections.emptySet();
    }

    @Override
    public FoundationDBPersistence initialize(String defaultNamespace, Map<String, String> configuration, Set<String> managedDomains) {
        FDB fdb = FDB.selectAPIVersion(520);
        Database db = fdb.open();
        Directory directory = DirectoryLayer.createWithContentSubspace(new Subspace(Tuple.from(defaultNamespace)));
        /*
        List<CompletableFuture<DirectorySubspace>> futures = new LinkedList<>();
        for (String managedDomain : managedDomains) {
            futures.add(directory.createOrOpen(db, PathUtil.from(managedDomain)));
        }
        Map<String, DirectorySubspace> directorySubspaceByDomain = new LinkedHashMap<>();
        for (CompletableFuture<DirectorySubspace> future : futures) {
            try {
                DirectorySubspace directorySubspace = future.get();
                List<String> path = directorySubspace.getPath();
                directorySubspaceByDomain.put(path.get(path.size() - 1), directorySubspace);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) e.getCause();
                }
                if (e.getCause() instanceof Error) {
                    throw (Error) e.getCause();
                }
                throw new RuntimeException(e.getCause());
            }
        }
        */
        FoundationDBPersistence persistence = new FoundationDBPersistence(db, defaultNamespace, directory, null);
        return persistence;
    }
}
