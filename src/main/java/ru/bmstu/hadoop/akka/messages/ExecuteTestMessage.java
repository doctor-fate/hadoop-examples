package ru.bmstu.hadoop.akka.messages;

public class ExecuteTestMessage {
    public final int id;
    public final String script;
    public final String function;
    public final ExecutePackMessage.Test test;

    public ExecuteTestMessage(int id, String script, String function, ExecutePackMessage.Test test) {
        this.id = id;
        this.script = script;
        this.function = function;
        this.test = test;
    }
}