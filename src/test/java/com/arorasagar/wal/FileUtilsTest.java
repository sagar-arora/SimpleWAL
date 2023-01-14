package com.arorasagar.wal;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;

public class FileUtilsTest {

    @Test
    public void test1() throws IOException {
        FileUtils.listLogFiles(Paths.get("."));
    }
}
