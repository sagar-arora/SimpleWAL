package com.arorasagar.wal;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class SimpleWriteAheadLogTest {

    private SimpleWriteAheadLog writeAheadLog;
    private final long timestamp = System.currentTimeMillis();

    private final WALEntry WAL_ENTRY = WALEntry
            .builder()
            .index(1)
            .entryType(EntryType.SET)
            .key("key".getBytes(StandardCharsets.UTF_8))
            .value("value".getBytes(StandardCharsets.UTF_8))
            .timestamp(timestamp)
            .build();

    @Before
    public void setup() throws IOException {
        writeAheadLog = new SimpleWriteAheadLog();
    }

    @Test
    public void testSerializeDeserialize() throws IOException {
        String key = "key", value = "value";

        byte[] bytes = writeAheadLog.serialize(1, EntryType.SET, key.getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8), timestamp);

        WALEntry walEntry = writeAheadLog.deserialize(ByteBuffer.wrap(bytes));

        Assert.assertEquals(WAL_ENTRY, walEntry);
    }
}
