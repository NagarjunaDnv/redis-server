package org.example.redisserver.types;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class SortedList {
    private ConcurrentSkipListSet<SkipListEntry> scoreMemberListSet = new ConcurrentSkipListSet<>();
    private HashMap<String, Double> members = new HashMap<>();

    public void setMembers(HashMap<String, Double> members) {
        this.members = members;
    }

    public HashMap<String, Double> getMembers() {
        return members;
    }

    public ConcurrentSkipListSet<SkipListEntry> getScoreMemberListSet() {
        return scoreMemberListSet;
    }

    public void setScoreMemberListSet(ConcurrentSkipListSet<SkipListEntry> scoreMemberListSet) {
        this.scoreMemberListSet = scoreMemberListSet;
    }

    public void add(String member, Double score) {
        scoreMemberListSet.remove(new SkipListEntry(member, score));
        scoreMemberListSet.add(new SkipListEntry(member, score));
        members.put(member, score);
    }
}
