package com.arorasagar.wal;

import lombok.*;

import java.io.Serializable;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Getter
public class WALEntry implements Serializable {
    private long index;
    private byte[] key;
    private byte[] value;
    private EntryType entryType;
    private long timestamp;
}
