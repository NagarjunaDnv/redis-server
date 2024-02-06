package org.example.redisserver.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.hrakaroo.glob.GlobPattern;
import com.hrakaroo.glob.MatchingEngine;
import org.example.redisserver.utils.DataStoreDeserializer;
import org.example.redisserver.types.KeyMetadata;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.*;

@JsonDeserialize(using = DataStoreDeserializer.class)
public class DataStore implements Serializable {
    private final Map<String, Object> data;
    private final Map<String, KeyMetadata> keysMetadata;

    public DataStore() {
        this.data = new HashMap<>();
        this.keysMetadata = new HashMap<>();
    }

    public DataStore(Map<String, Object> data, Map<String, KeyMetadata> keysMetadata) {
        this.data = data;
        this.keysMetadata = keysMetadata;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public Map<String, KeyMetadata> getKeysMetadata() {
        return keysMetadata;
    }

    public void serialize(String fileName) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writeValue(new File(fileName), this);
        } catch (IOException e) {
            System.out.println("Error occurred while writing to disc");
        }
    }

    public DataStore deserialize(String fileName) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(new File(fileName), DataStore.class);
        } catch (IOException e) {
            System.out.println("Error occurred while fetching from disc");
            return null;
        }
    }

    public void removeExpiredKeys() {
        Random random = new Random();
        int totalToBeExpired = random.nextInt(10, 100);

        Iterator<Map.Entry<String, KeyMetadata>> iterator = keysMetadata.entrySet().iterator();

        while (totalToBeExpired > 0 && iterator.hasNext()) {
            Map.Entry<String, KeyMetadata> entry = iterator.next();
            if (entry.getValue().getExpiry() != null && entry.getValue().getExpiry().isBefore(Instant.now())) {
                iterator.remove();
                data.remove(entry.getKey());
                totalToBeExpired--;
            }
        }
    }

    public void merge(DataStore loadedDataStore) {
        this.data.putAll(loadedDataStore.data);
        this.keysMetadata.putAll(loadedDataStore.keysMetadata);
    }

    public boolean containsKey(String key) {
        return data.containsKey(key);
    }

    public String getValue(String key) {
        return (String) data.get(key);
    }

    public void setValue(String key, Object value) {
        data.put(key, value);
    }

    public int deleteKey(String key) {
        if(data.containsKey(key)) {
            data.remove(key);
            keysMetadata.remove(key);
            return 1;
        }
        else {
            return 0;
        }
    }

    public KeyMetadata getKeyMetadata(String key) {
        return keysMetadata.get(key);
    }

    public void setKeyMetadata(String key, KeyMetadata keyMetadata) {
        keysMetadata.put(key, keyMetadata);
    }

    public List<String> getMatchingKeys(String globePattern) {
        MatchingEngine m = GlobPattern.compile(globePattern);

        List<String> result = new ArrayList<>();

        for(String key: data.keySet()) {
            if(m.matches(key)) {
                result.add(key);
            }
        }

        return result;
    }

    public Object get(String key) {
        return data.get(key);
    }
}
