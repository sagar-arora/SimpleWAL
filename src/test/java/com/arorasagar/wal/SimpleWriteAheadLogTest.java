package com.arorasagar.wal;

import com.arorasagar.wal.exception.WALException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class SimpleWriteAheadLogTest {

    SimpleWriteAheadLog writeAheadLog;
    WALEntry walEntry = WALEntry.builder()
            .entryType(EntryType.SET)
            .key("key".getBytes(StandardCharsets.UTF_8))
            .value("value".getBytes(StandardCharsets.UTF_8))
            .build();

    @Before
    public void setup() throws IOException {
        writeAheadLog = new SimpleWriteAheadLog();
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
}
