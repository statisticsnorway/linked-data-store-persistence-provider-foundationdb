package no.ssb.lds.core.persistence.foundationdb;

import no.ssb.lds.api.persistence.buffered.DefaultBufferedPersistence;
import no.ssb.lds.core.persistence.test.BufferedPersistenceIntegration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.Map;
import java.util.Set;

public class FoundationDBIntegrationTest extends BufferedPersistenceIntegration {

    public FoundationDBIntegrationTest() {
        super("lds-provider-fdb-testng-ns");
    }

    @BeforeClass
    public void setup() {
        streaming = new FoundationDBInitializer().initialize(
                namespace,
                Map.of("foundationdb.directory.node-prefix.hex", "3A",
                        "foundationdb.directory.content-prefix.hex", "3B"),
                Set.of("Person", "Address", "FunkyLongAddress"));
        persistence = new DefaultBufferedPersistence(streaming);
    }

    @AfterClass
    public void teardown() {
        if (persistence != null) {
            persistence.close();
        }
    }
}
