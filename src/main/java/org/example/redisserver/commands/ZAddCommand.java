package org.example.redisserver.commands;

import org.example.redisserver.data.DataProcessor;
import org.example.redisserver.types.DataType;
import org.example.redisserver.types.KeyMetadata;
import org.example.redisserver.types.SortedList;
import org.example.resp.resp3.RESP3Encoder;

import java.util.Objects;

public class ZAddCommand implements IRedisCommand {

    private final DataProcessor dataProcessor;

    public ZAddCommand(DataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }

    @Override
    public String execute(String[] args) {
        int size = args.length;

        if (size < 3) {
            return RESP3Encoder.encodeError("ERR ZADD requires at least a key, score and a member.");
        }

        String key = args[0];
        boolean keyExists = dataProcessor.containsKey(key);

        if(keyExists && dataProcessor.getKeyMetadata(key).getDataType() != DataType.SORTED_LIST) {
            return RESP3Encoder.encodeError("WRONGTYPE Wrong type");
        }

        SortedList sortedList = dataProcessor.get(key);

        if(sortedList == null) {
            sortedList = new SortedList();
        }

        boolean onlyUpdateExistingElements = false;
        boolean onlyAddNewElements = false;
        boolean updateExistingElementsIfScoreIsLess = false;
        boolean updateExistingElementsIfScoreIsMore = false;
        boolean returnToIncludeUpdatedElements = false;
        boolean incr = false;


        int addedElementsCount = 0;
        int changedElementsCount = 0;

        int i = 1;

        while (i < args.length) {

            if(Objects.equals(args[i], "XX")) {
                onlyUpdateExistingElements = true;
            }
            else if(Objects.equals(args[i], "NX")) {
                onlyAddNewElements = true;
            }
            else if(Objects.equals(args[i], "GT")) {
                updateExistingElementsIfScoreIsMore = true;
            }
            else if(Objects.equals(args[i], "LT")) {
                updateExistingElementsIfScoreIsLess = true;
            }
            else if(Objects.equals(args[i], "CH")) {
                returnToIncludeUpdatedElements = true;
            }
            else if(Objects.equals(args[i], "INCR")) {
                incr = true;
            }
            else{
                break;
            }

            i++;
        }


        if(incr) {

            if(i == args.length - 2) {

                Double increment = Double.parseDouble(args[i]);
                String member = args[i+1];

                //Get score by member (O(1))
                Double oldScore = sortedList.getMembers().get(member);

                //Increment the score
                if(onlyAddNewElements) {
                    if(oldScore == null) {
                        sortedList.add(member, increment);
                        return RESP3Encoder.encodeDouble(increment);
                    }
                    else {
                        return RESP3Encoder.encodeNull();
                    }
                }
                else {
                    Double newScore = oldScore == null ? increment : oldScore + increment;
                    sortedList.add(member, newScore);
                    return RESP3Encoder.encodeDouble(newScore);
                }

            }
            else if(i < args.length - 2) {
                return RESP3Encoder.encodeError("ERR INCR option supports a single increment-element pair");
            }
            else {
                return RESP3Encoder.encodeError("ERR syntax error");
            }
        }


        //At this point, we must have reached the score-member pairs
        while(i < args.length) {
            try {
                Double score = Double.parseDouble(args[i]);
                String member = args[i+1];

                //Get score by member (O(1))
                Double oldScore = sortedList.getMembers().get(member);

                if(onlyAddNewElements) {
                    if(oldScore == null) {
                        sortedList.add(member, score);
                        addedElementsCount++;
                    }
                }

                if(updateExistingElementsIfScoreIsLess) {
                    if(oldScore != null && score.compareTo(oldScore) < 0) {
                        sortedList.add(member, score);
                        changedElementsCount++;
                    }
                }
                else if (updateExistingElementsIfScoreIsMore) {
                    if(oldScore != null && score.compareTo(oldScore) > 0) {
                        sortedList.add(member, score);
                        changedElementsCount++;
                    }
                }
                else if(onlyUpdateExistingElements) {
                    if(oldScore != null && !oldScore.equals(score)) {
                        sortedList.add(member, score);
                        changedElementsCount++;
                    }
                }
                else {

                    if(oldScore != null) {
                        if(!oldScore.equals(score)) {
                            sortedList.add(member, score);
                            changedElementsCount++;
                        }
                    }
                    else {
                        sortedList.add(member, score);
                        addedElementsCount++;
                    }
                }

                i += 2;
            }
            catch (Exception e) {
                return RESP3Encoder.encodeError("ERR Syntax error occurred");
            }
        }

        dataProcessor.setValue(key, sortedList);
        dataProcessor.setKeyMetadata(key, new KeyMetadata(null, DataType.SORTED_LIST));

        if(returnToIncludeUpdatedElements) {
            return RESP3Encoder.encodeInteger(addedElementsCount + changedElementsCount);
        }
        else {
            return RESP3Encoder.encodeInteger(addedElementsCount);
        }
    }

}
