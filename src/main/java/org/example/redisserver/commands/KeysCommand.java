package org.example.redisserver.commands;

import org.example.redisserver.data.DataProcessor;
import org.example.resp.resp3.RESP3Encoder;

public class KeysCommand implements IRedisCommand {

    private final DataProcessor dataProcessor;

    public KeysCommand(DataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }

    @Override
    public String execute(String[] args) {

        int size = args.length;

        if (size < 1) {
            return RESP3Encoder.encodeError("ERR KEYS requires a pattern");
        }

        String pattern = args[0];
        return RESP3Encoder.encode(dataProcessor.getMatchingKeys(pattern));
    }
}
