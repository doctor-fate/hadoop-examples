package ru.bmstu.hadoop.akka.messages;

public class ExecuteTestMessage {
    public final int id;
    public final String script;
    public final String function;
    public final ExecutePackMessage.Test test;

    public ExecuteTestMessage(ExecutePackMessage message, ExecutePackMessage.Test test) {
        this.id = message.id;
        this.script = message.script;
        this.function = message.function;
        this.test = test;
    }
}