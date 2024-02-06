package org.example.redisserver.commands;

public interface IRedisCommand {
    String execute(String[] args);
}
