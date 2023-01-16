package com.arorasagar.wal;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class TestUtil {
    private TestUtil() {}

    public static Path createTempDirectory() throws IOException {
        Path directory = Files.createTempDirectory(Paths.get("."), "temp");
        directory.toFile().deleteOnExit();
        return directory;
    }

    public static Path createTempFileInDirectory(Path directory, String fileName) throws IOException {
        Path file = Files.createFile(Paths.get(directory.toAbsolutePath() + File.separator + fileName));
        file.toFile().deleteOnExit();
        return file;
    }
}
