package org.example.redisserver.commands;

import org.example.redisserver.data.DataProcessor;
import org.example.resp.resp3.RESP3Encoder;

import java.time.Instant;

public class GetCommand implements IRedisCommand {

    private final DataProcessor dataProcessor;

    public GetCommand(DataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }

    @Override
    public String execute(String... args) {

        int size = args.length;

        if (size < 1) {
            return RESP3Encoder.encodeError("ERR GET requires a key");
        }

        String key = args[0];

        //Check if it is already expired
        if(!dataProcessor.containsKey(key)) {
            return RESP3Encoder.encodeNull();
        }

        if(dataProcessor.isExpired(key)) {
            return RESP3Encoder.encodeNull();
        }

        String value = dataProcessor.getValue(key);


        if(value == null) {
            return RESP3Encoder.encodeNull();
        }
        else {
            return RESP3Encoder.encodeBulkString(value);
        }
    }
}
