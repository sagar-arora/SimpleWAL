package com.arorasagar.wal;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.nio.charset.StandardCharsets;


public class WALIteratorTest {

    private final long timestamp = 1000;
    private final WALEntry WAL_ENTRY = WALEntry
            .builder()
            .index(1)
            .entryType(EntryType.DELETE)
            .key("test".getBytes(StandardCharsets.UTF_8))
            .value("test".getBytes(StandardCharsets.UTF_8))
            .timestamp(timestamp)
            .build();

    @Test
    public void testWalIterator() throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
       DataOutputStream oos = new DataOutputStream(bos);
       oos.writeInt(1);
       oos.writeByte(1);
       oos.writeLong(4);
       oos.writeBytes("test");
       oos.writeLong(4);
       oos.writeBytes("test");
       oos.writeLong(1000);
       oos.flush();
       byte[] data = bos.toByteArray();

       WALIterator walIterator = new WALIterator(data);

       Assert.assertEquals(WAL_ENTRY, walIterator.next());
    }
}
