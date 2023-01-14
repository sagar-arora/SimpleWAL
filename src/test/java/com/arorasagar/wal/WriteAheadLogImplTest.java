package com.arorasagar.wal;

import com.arorasagar.wal.exception.WALException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class WriteAheadLogImplTest {

    WriteAheadLogImpl writeAheadLog;

    @Before
    public void setup() throws IOException {
        writeAheadLog = new WriteAheadLogImpl();
    }

    @Test
    public void test1() throws IOException, WALException {
        String key = "key", value = "value";

        writeAheadLog.operation(EntryType.SET, key.getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8));

        writeAheadLog.flush();
    }
}
