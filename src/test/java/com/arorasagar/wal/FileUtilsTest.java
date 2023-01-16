package com.arorasagar.wal;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class FileUtilsTest {

    @Test
    public void test1() throws IOException {
        File file = Files.createTempFile(Paths.get(".").toAbsolutePath(), "wal-1234", ".log").toFile();
        file.deleteOnExit();
        Assert.assertEquals(1, FileUtils.listLogFiles(Paths.get(".")).size());
    }

    @Test
    public void test() throws IOException {
        File file1 = Files.createTempFile(Paths.get(".").toAbsolutePath(), "wal-", ".log").toFile();
        file1.deleteOnExit();
        File file2 = Files.createTempFile(Paths.get(".").toAbsolutePath(), "wal-", ".log").toFile();
        file2.deleteOnExit();
        File file3 = Files.createTempFile(Paths.get(".").toAbsolutePath(), "wal-", ".log").toFile();
        file3.deleteOnExit();
        Assert.assertEquals(1, FileUtils.listLogFiles(Paths.get(".")).size());
    }

    @Test
    public void testFileSorting() throws IOException {
        Path directory = TestUtil.createTempDirectory();
        Path path1 = TestUtil.createTempFileInDirectory(directory, "wal-1235.log");
        Path path2 = TestUtil.createTempFileInDirectory(directory, "wal-234.log");
        Path path3 = TestUtil.createTempFileInDirectory(directory, "wal-123.log");

        List<FileMetadata> files = FileUtils.sortFilesByTimestamp(FileUtils.listLogFiles(directory.toAbsolutePath()));

        for (FileMetadata fileMetadata : files) {
            System.out.println(fileMetadata.toString());
        }

    }
}
