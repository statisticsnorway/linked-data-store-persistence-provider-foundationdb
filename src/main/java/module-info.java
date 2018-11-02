import no.ssb.lds.api.persistence.PersistenceInitializer;

module no.ssb.lds.persistence.neo4j {
    requires no.ssb.lds.persistence.api;
    requires org.json;
    requires java.logging;
    requires jul_to_slf4j;
    requires fdb.java;

    provides PersistenceInitializer with no.ssb.lds.core.persistence.foundationdb.FoundationDBInitializer;
}
