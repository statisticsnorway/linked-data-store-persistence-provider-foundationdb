package no.ssb.lds.core.persistence.foundationdb;

import no.ssb.lds.api.persistence.TransactionStatistics;

class FoundationDBStatistics extends TransactionStatistics {

    public FoundationDBStatistics clearRange(String index) {
        add(index + " clear range", 1);
        return this;
    }

    public FoundationDBStatistics clearKeyValue(String index) {
        add(index + " clear key-value", 1);
        return this;
    }

    public FoundationDBStatistics setKeyValue(String index) {
        add(index + " set key-value", 1);
        return this;
    }

    public FoundationDBStatistics getRange(String index) {
        add(index + " get range", 1);
        return this;
    }

    public FoundationDBStatistics rangeIteratorNext(String index) {
        add(index + " range-iterator next", 1);
        return this;
    }

    public FoundationDBStatistics rangeAsList(String index) {
        add(index + " range-iterator asList", 1);
        return this;
    }

    public FoundationDBStatistics rangeIteratorCancel(String index) {
        add(index + " range-iterator cancel", 1);
        return this;
    }
}
