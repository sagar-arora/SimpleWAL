package com.arorasagar.wal;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class FileUtilsTest {

    @Test
    public void testFileSorting() throws IOException {
        Path directory = TestUtil.createTempDirectory();
        Path path1 = TestUtil.createTempFileInDirectory(directory, "wal-1235.log");
        Path path2 = TestUtil.createTempFileInDirectory(directory, "wal-234.log");
        Path path3 = TestUtil.createTempFileInDirectory(directory, "wal-123.log");

        List<FileMetadata> files = FileUtils.sortFilesByTimestamp(FileUtils.listLogFiles(directory.toAbsolutePath()));

        Assert.assertEquals("wal-1235.log", files.get(0).getFilePath().getFileName().toString());
        Assert.assertEquals("wal-234.log", files.get(1).getFilePath().getFileName().toString());
        Assert.assertEquals("wal-123.log", files.get(2).getFilePath().getFileName().toString());
    }
}
