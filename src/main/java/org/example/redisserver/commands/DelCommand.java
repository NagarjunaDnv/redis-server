package org.example.redisserver.commands;

import org.example.redisserver.data.DataProcessor;
import org.example.resp.resp3.RESP3Encoder;

public class DelCommand implements IRedisCommand {

    private final DataProcessor dataProcessor;

    public DelCommand(DataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }

    @Override
    public String execute(String[] args) {
        int size = args.length;

        if (size < 1) {
            return RESP3Encoder.encodeError("ERR DEL requires a key");
        }

        int totalDeletions = 0;

        for(int i =0; i < args.length; i++) {
            totalDeletions += dataProcessor.delete(args[i]);
        }


        return RESP3Encoder.encodeInteger(totalDeletions);
    }
}
