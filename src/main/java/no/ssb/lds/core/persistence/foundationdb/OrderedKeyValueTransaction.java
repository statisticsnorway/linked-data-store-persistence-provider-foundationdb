package no.ssb.lds.core.persistence.foundationdb;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.async.AsyncIterable;
import no.ssb.lds.api.persistence.Transaction;

public interface OrderedKeyValueTransaction extends Transaction {

    void clearRange(Range range, String index);

    void set(byte[] key, byte[] value, String index);

    AsyncIterable<KeyValue> getRange(Range range, String index);

    AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, String index);

    AsyncIterable<KeyValue> getRange(KeySelector begin, KeySelector end, int limit, StreamingMode streamingMode, String index);

    void clear(byte[] key, String index);
}
