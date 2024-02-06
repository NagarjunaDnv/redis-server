package org.example.redisserver.data;

import org.example.redisserver.commands.*;
import org.example.redisserver.types.KeyMetadata;
import org.example.redisserver.types.SortedList;
import org.example.resp.resp3.RESP3Encoder;
import org.example.resp.resp3.types.CommandType;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class DataProcessor {
    private final DataStore dataStore;

    public DataProcessor(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    public void persist(String fileName) {
        this.dataStore.serialize(fileName);
    }

    public void load(String fileName) {
        DataStore loadedDataStore = this.dataStore.deserialize(fileName);

        if(loadedDataStore != null) {
            this.dataStore.merge(loadedDataStore);
        }
    }

    public String executeCommand(Object[] args) {

        if(args.length < 1) {
            return RESP3Encoder.encodeError("ERR Command should be present");
        }

        try {
            CommandType command = CommandType.valueOf((String) args[0]);

            return switch (command) {
                case SET -> new SetCommand(this).execute(Arrays.copyOfRange(args, 1, args.length, String[].class));
                case GET -> new GetCommand(this).execute(Arrays.copyOfRange(args, 1, args.length, String[].class));
                case DEL -> new DelCommand(this).execute(Arrays.copyOfRange(args, 1, args.length, String[].class));
                case EXPIRE -> new ExpireCommand(this).execute(Arrays.copyOfRange(args, 1, args.length, String[].class));
                case KEYS -> new KeysCommand(this).execute(Arrays.copyOfRange(args, 1, args.length, String[].class));
                case TTL -> new TtlCommand(this).execute(Arrays.copyOfRange(args, 1, args.length, String[].class));
                case ZADD -> new ZAddCommand(this).execute(Arrays.copyOfRange(args, 1, args.length, String[].class));
                case ZRANGE -> new ZRangeCommand(this).execute(Arrays.copyOfRange(args, 1, args.length, String[].class));
            };
        }
        catch (Exception e) {
            return RESP3Encoder.encodeError(e.getMessage());
        }

    }

    public void removeExpiredKeys() {
        this.dataStore.removeExpiredKeys();
    }

    public boolean containsKey(String key) {
        return dataStore.containsKey(key);
    }

    public String getValue(String key) {
        return (String) dataStore.getValue(key);
    }

    public int delete(String key) {
        return dataStore.deleteKey(key);
    }

    public void setValue(String key, Object value) {
        dataStore.setValue(key, value);
    }

    public KeyMetadata getKeyMetadata(String key) {
        return dataStore.getKeyMetadata(key);
    }

    public void setKeyMetadata(String key, KeyMetadata keyMetadata) {
        dataStore.setKeyMetadata(key, keyMetadata);
    }

    public List<String> getMatchingKeys(String globPattern) {
        return dataStore.getMatchingKeys(globPattern);
    }

    public boolean isExpired(String key) {
        KeyMetadata keyMetadata = dataStore.getKeyMetadata(key);

        if(keyMetadata == null) {
            return true;
        }

        return keyMetadata.getExpiry() != null && keyMetadata.getExpiry().isBefore(Instant.now());
    }

    public SortedList get(String key) {
        return (SortedList) this.dataStore.get(key);
    }
}
