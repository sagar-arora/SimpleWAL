package com.arorasagar.wal;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;


public class WALIteratorTest {

    public WALIteratorTest() {

    }

    @Test
    public void test1() throws IOException {
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
       System.out.println(Arrays.toString(data));

       WALIterator walIterator = new WALIterator(data);
       System.out.println(walIterator.next());
    }
}
