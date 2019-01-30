package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.subspace.Subspace;
import no.ssb.lds.api.persistence.Transaction;

public interface OrderedKeyValueTransaction extends Transaction {

    void clearRange(Range range, String index);

    void set(byte[] key, byte[] value, String index);

    void clear(byte[] key, String index);

    AsyncIterable<KeyValue> getRange(Range range, String index, int limit);

    AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, String index, int limit);

    AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, StreamingMode streamingMode, String index);

    void dumpIndex(String index, Subspace subspace);
}
