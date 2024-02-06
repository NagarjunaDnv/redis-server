package org.example.redisserver.types;

import java.time.Instant;

public class KeyMetadata {
    private Instant expiry;
    private DataType dataType;

    public KeyMetadata(Instant expiry, DataType dataType) {
        this.expiry = expiry;
        this.dataType = dataType;
    }

    public KeyMetadata() {

    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public void setExpiry(Instant expiry) {
        this.expiry = expiry;
    }

    public Instant getExpiry() {
        return expiry;
    }

    public DataType getDataType() {
        return dataType;
    }
}
