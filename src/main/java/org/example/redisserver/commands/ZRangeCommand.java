package org.example.redisserver.commands;

import org.example.redisserver.data.DataProcessor;
import org.example.redisserver.types.DataType;
import org.example.redisserver.types.SkipListEntry;
import org.example.redisserver.types.SortedList;
import org.example.resp.resp3.RESP3Encoder;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

public class ZRangeCommand implements IRedisCommand {

    private final DataProcessor dataProcessor;

    public ZRangeCommand(DataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }

    @Override
    public String execute(String[] args) {

        int size = args.length;

        if (size < 3) {
            return RESP3Encoder.encodeError("ERR ZADD requires at least a key, start and a stop.");
        }

        String key = args[0];
        boolean keyExists = dataProcessor.containsKey(key);

        if(keyExists && dataProcessor.getKeyMetadata(key).getDataType() != DataType.SORTED_LIST) {
            return RESP3Encoder.encodeError("WRONGTYPE Wrong type");
        }

        if(!keyExists) {
            return RESP3Encoder.encodeError("ERR Key not found");
        }

        boolean startInclusive = !args[1].startsWith("(");
        boolean endInclusive = !args[2].startsWith("(");

        Double start = startInclusive ? Double.parseDouble(args[1]) : Double.parseDouble(args[1].substring(1));
        Double end = endInclusive ? Double.parseDouble(args[2]) : Double.parseDouble(args[2].substring(1));

        boolean byLex = false;
        boolean byScore = false;
        boolean rev = false;
        int offset = 0;
        int limit = Integer.MAX_VALUE;
        boolean includeScoresInResult = false;

        int i = 3;
        while(i < args.length) {
            if(args[i].equals("BYLEX")) {
                byLex = true;
            }
            else if(args[i].equals("BYSCORE")) {
                byScore = true;
            }
            else if(args[i].equals("REV")) {
                rev = true;
            }
            else if(args[i].equals("LIMIT")) {
                offset = Integer.parseInt(args[i+1]);
                limit = Integer.parseInt(args[i+2]);
                i += 2;
            }
            else if(args[i].equals("WITHSCORES")) {
                includeScoresInResult = true;
            }

            i++;
        }

        SortedList sortedList = dataProcessor.get(key);
        List<String> result = new ArrayList<>();

        if(byScore) {

            NavigableSet<SkipListEntry> subset =  sortedList
                    .getScoreMemberListSet()
                    .subSet(
                            new SkipListEntry("", start),
                            startInclusive,
                            new SkipListEntry("", end),
                            endInclusive
                    );

            if(rev) {
                subset = subset.descendingSet();
            }

            Iterator<SkipListEntry> skipListEntryIterator = subset.stream().skip(offset).iterator();;

            int count = 0;
            while (skipListEntryIterator.hasNext()) {
                SkipListEntry entry = skipListEntryIterator.next();
                if(count < limit){
                    result.add(entry.getMember());
                    if(includeScoresInResult) {
                        result.add(entry.getScore().toString());
                    }
                }
                count++;
            }
        }
        else {
            NavigableSet<SkipListEntry> set = sortedList.getScoreMemberListSet();

            if(start <= end && start <= set.size() - 1) {
                int index = 0;
                for(SkipListEntry entry: set) {
                    if(start <= index && index <= end){
                        if(index >= offset && (index - start) < limit) {
                            result.add(entry.getMember());
                            if(includeScoresInResult) {
                                result.add(entry.getScore().toString());
                            }
                        }
                    }
                    index++;
                }
            }
        }

        return RESP3Encoder.encode(result);
    }
}
