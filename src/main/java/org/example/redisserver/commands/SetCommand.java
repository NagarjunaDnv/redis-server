package org.example.redisserver.commands;

import org.example.redisserver.data.DataProcessor;
import org.example.redisserver.types.DataType;
import org.example.redisserver.types.KeyMetadata;
import org.example.resp.resp3.RESP3Encoder;

import java.time.Instant;

public class SetCommand implements IRedisCommand {
    private final DataProcessor dataProcessor;

    public SetCommand(DataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }

    @Override
    public String execute(String[] args) {

        int size = args.length;

        if (size < 2) {
            return RESP3Encoder.encodeError("ERR SET requires at least a key and a value.");
        }

        String key = args[0];
        String value = args[1];

        boolean keyExists = dataProcessor.containsKey(key);
        boolean setOnlyIfDoesnotExist = false;
        boolean setOnlyIfExists = false;
        boolean hasGet = false;
        boolean keepTTL = false;
        Instant expirationTime = null;

        for(int i = 2; i < size; i++) {
            String option = args[i];

            switch (option) {
                case "NX" -> {
                    setOnlyIfDoesnotExist = true;
                }
                case "XX" -> {
                    setOnlyIfExists = true;
                }
                case "GET" -> {
                    hasGet = true;
                }
                case "EX" -> {
                    if (i+1 < size) {
                        long seconds = Long.parseLong(args[i + 1]);
                        expirationTime = Instant.now().plusSeconds(seconds);
                        i++;
                    }
                }
                case "PX" -> {
                    if (i + 1 < args.length) {
                        long milliseconds = Long.parseLong(args[i + 1]);
                        expirationTime = Instant.now().plusMillis(milliseconds);
                        i++;
                    }
                }
                case "EXAT" -> {
                    if (i + 1 < args.length) {
                        long unixTimeSeconds = Long.parseLong(args[i + 1]);
                        expirationTime = Instant.ofEpochSecond(unixTimeSeconds);
                        i++;
                    }
                }
                case "PXAT" -> {
                    if (i + 1 < args.length) {
                        long unixTimeMilliseconds = Long.parseLong(args[i + 1]);
                        expirationTime = Instant.ofEpochMilli(unixTimeMilliseconds);
                        i++;
                    }
                }
                case "KEEPTTL" -> {
                    keepTTL = true;
                }
                default -> {}
            }

        }

        if(keepTTL) {
            KeyMetadata keyMetadata = dataProcessor.getKeyMetadata(key);
            expirationTime = keyMetadata != null ? keyMetadata.getExpiry() : null;
        }

        String oldValue = dataProcessor.getValue(key);

        if(!((setOnlyIfExists && !keyExists) || (setOnlyIfDoesnotExist && keyExists))) {
            dataProcessor.setValue(key, value);
            dataProcessor.setKeyMetadata(key, new KeyMetadata(expirationTime, DataType.STRING));
        }

        if(hasGet) {
            return oldValue != null ? RESP3Encoder.encodeBulkString(oldValue) : RESP3Encoder.encodeNull();
        }

        return RESP3Encoder.encodeSimpleString("OK");
    }
}
