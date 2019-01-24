import no.ssb.lds.api.persistence.PersistenceInitializer;

module no.ssb.lds.persistence.foundationdb {
    requires no.ssb.lds.persistence.api;
    requires java.logging;
    requires jul_to_slf4j;
    requires fdb.java;
    requires org.slf4j;

    exports no.ssb.lds.core.persistence.foundationdb;

    provides PersistenceInitializer with no.ssb.lds.core.persistence.foundationdb.FoundationDBInitializer;
}
