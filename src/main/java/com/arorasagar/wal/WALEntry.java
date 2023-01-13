package com.arorasagar.wal;

import lombok.*;

import java.io.Serializable;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class WALEntry implements Serializable {
    private long index;
    byte[] key;
    byte[] value;
    EntryType entryType;
    private long timestamp;
}
