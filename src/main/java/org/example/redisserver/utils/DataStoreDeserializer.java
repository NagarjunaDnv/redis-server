package org.example.redisserver.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.example.redisserver.data.DataStore;
import org.example.redisserver.types.KeyMetadata;
import org.example.redisserver.types.SortedList;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DataStoreDeserializer extends StdDeserializer<DataStore> {

    public DataStoreDeserializer() {
        this(null);
    }

    public DataStoreDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public DataStore deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        JsonNode node = jp.getCodec().readTree(jp);

        Map<String, Object> data = new HashMap<>();
        Map<String, KeyMetadata> keysMetadata = new HashMap<>();

        // Deserialize 'data' map
        JsonNode dataNode = node.get("data");
        for (Iterator<Map.Entry<String, JsonNode>> it = dataNode.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            String key = entry.getKey();
            JsonNode valueNode = entry.getValue();

            // Check if the value is a SortedList
            if (valueNode.has("scoreMemberListSet") && valueNode.has("members")) {
                // Deserialize as SortedList
                SortedList sortedList = jp.getCodec().treeToValue(valueNode, SortedList.class);
                data.put(key, sortedList);
            } else {
                // Deserialize as String or other type as needed
                data.put(key, jp.getCodec().treeToValue(valueNode, Object.class));
            }
        }

        // Deserialize 'keysMetadata' map
        JsonNode keysMetadataNode = node.get("keysMetadata");
        for (Iterator<Map.Entry<String, JsonNode>> it = keysMetadataNode.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            String key = entry.getKey();
            JsonNode valueNode = entry.getValue();
            KeyMetadata keyMetadata = jp.getCodec().treeToValue(valueNode, KeyMetadata.class);
            keysMetadata.put(key, keyMetadata);
        }

        return new DataStore(data, keysMetadata);
    }
}

