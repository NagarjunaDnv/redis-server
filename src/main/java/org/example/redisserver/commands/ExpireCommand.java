package org.example.redisserver.commands;

import org.example.redisserver.data.DataProcessor;
import org.example.redisserver.types.KeyMetadata;
import org.example.resp.resp3.RESP3Encoder;

import java.time.Instant;

public class ExpireCommand implements IRedisCommand {

    private final DataProcessor dataProcessor;

    public ExpireCommand(DataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }

    @Override
    public String execute(String[] args) {

        int size = args.length;

        if (size < 2) {
            return RESP3Encoder.encodeError("ERR EXPIRE requires a key and time in seconds.");
        }

        String key = args[0];
        Instant newExpiry = Instant.now().plusSeconds(Long.parseLong(args[1]));

        boolean keyExists = dataProcessor.containsKey(key);

        if(!keyExists) {
            return RESP3Encoder.encodeError("ERR key not found");
        }

        boolean setOnlyIfDoesnotExist = false;
        boolean setOnlyIfExists = false;
        boolean setOnlyIfNewExpiryIsGreater = false;
        boolean setOnlyIfNewExpiryIsLesser = false;

        for(int i = 2; i < size; i++) {
            String option = args[i];

            switch (option) {
                case "NX" -> {
                    setOnlyIfDoesnotExist = true;
                }
                case "XX" -> {
                    setOnlyIfExists = true;
                }
                case "GT" -> {
                    setOnlyIfNewExpiryIsGreater = true;
                }
                case "LT" -> {
                    setOnlyIfNewExpiryIsLesser = true;
                }
                default -> {}
            }
        }

        KeyMetadata curMetadata = dataProcessor.getKeyMetadata(key);
        boolean expiryExists = curMetadata.getExpiry() != null;

        if(setOnlyIfExists && !expiryExists) {
            return RESP3Encoder.encodeInteger(0);
        }

        if(setOnlyIfDoesnotExist && expiryExists) {
            return RESP3Encoder.encodeInteger(0);
        }

        if(setOnlyIfNewExpiryIsGreater && curMetadata.getExpiry().isAfter(newExpiry)) {
            return RESP3Encoder.encodeInteger(0);
        }

        if(setOnlyIfNewExpiryIsLesser && curMetadata.getExpiry().isBefore(newExpiry)) {
            return RESP3Encoder.encodeInteger(0);
        }

        curMetadata.setExpiry(newExpiry);
        dataProcessor.setKeyMetadata(key, curMetadata);

        return RESP3Encoder.encodeInteger(1);
    }
}
