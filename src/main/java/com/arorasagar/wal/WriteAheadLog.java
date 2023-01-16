package com.arorasagar.wal;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

public interface WriteAheadLog {

    void open();

    void open(Path path);

    List<WALEntry> load();
}
