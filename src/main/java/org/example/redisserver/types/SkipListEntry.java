package org.example.redisserver.types;

public class SkipListEntry implements Comparable<SkipListEntry> {
    private String member;
    private Double score;

    public SkipListEntry(String member, Double score) {
        this.member = member;
        this.score = score;
    }

    public SkipListEntry() {}

    public void setMember(String member) {
        this.member = member;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public String getMember() {
        return member;
    }

    public Double getScore() {
        return score;
    }

    @Override
    public boolean equals(Object obj) {
        SkipListEntry skipListEntry = (SkipListEntry) obj;
        return skipListEntry.getScore().equals(this.score);
    }


    @Override
    public int compareTo(SkipListEntry anotherSkipListEntry) {
        return this.getScore().compareTo(anotherSkipListEntry.getScore());
    }
}
