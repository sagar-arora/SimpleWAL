package com.arorasagar.wal;

import com.arorasagar.wal.exception.WALException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

public class SimpleWriteAheadLogTest {

    SimpleWriteAheadLog writeAheadLog;
    private final long timestamp = System.currentTimeMillis();

    private SimpleWriteAheadLog.SimpleWriteAheadLogWriter writeAheadLogWriter;

    private final WALEntry WAL_ENTRY = WALEntry
            .builder()
            .index(1)
            .entryType(EntryType.SET)
            .key("key".getBytes(StandardCharsets.UTF_8))
            .value("value".getBytes(StandardCharsets.UTF_8))
            .timestamp(timestamp)
            .build();

    private File file;

    @Before
    public void setup() throws IOException {
        writeAheadLog = new SimpleWriteAheadLog();
        writeAheadLogWriter = new SimpleWriteAheadLog.SimpleWriteAheadLogWriter();
        Path directory = TestUtil.createTempDirectory();
        file = TestUtil.createTempFileInDirectory(directory, "wal-1235.log").toFile();
        writeAheadLog.open();
    }

    @Test
    public void test1() throws IOException, WALException {
        String key = "key", value = "value";

        writeAheadLog.operation(EntryType.SET, key.getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8));

        writeAheadLog.flush();

        List<WALEntry> entries = writeAheadLog.load();

        Assert.assertEquals(new String(entries.get(0).getKey()), "key");
    }

    @Test
    public void testSerializeDeserialize() throws IOException, WALException {
        String key = "key", value = "value";

        byte[] bytes = writeAheadLogWriter.searlize(EntryType.SET, key.getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8), timestamp);

        WALEntry walEntry = writeAheadLog.deserialize(ByteBuffer.wrap(bytes));

        Assert.assertEquals(WAL_ENTRY, walEntry);
    }
}
