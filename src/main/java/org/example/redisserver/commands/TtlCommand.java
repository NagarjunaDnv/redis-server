package org.example.redisserver.commands;

import org.example.redisserver.data.DataProcessor;
import org.example.redisserver.types.KeyMetadata;
import org.example.resp.resp3.RESP3Encoder;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class TtlCommand implements IRedisCommand {
    private final DataProcessor dataProcessor;

    public TtlCommand(DataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }

    @Override
    public String execute(String[] args) {
        int size = args.length;

        if (size < 1) {
            return RESP3Encoder.encodeError("ERR TTL requires a key");
        }

        String key = args[0];
        KeyMetadata keyMetadata = dataProcessor.getKeyMetadata(key);

        if(keyMetadata == null) {
            return RESP3Encoder.encodeInteger(-2);
        }

        if(keyMetadata.getExpiry() == null) {
            return RESP3Encoder.encodeInteger(-1);
        }

        return RESP3Encoder.encodeInteger(Instant.now().until(keyMetadata.getExpiry(), ChronoUnit.SECONDS));
    }
}
