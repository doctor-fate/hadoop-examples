package ru.bmstu.hadoop.akka.messages;

public class GetPackMessage {
    public final int id;

    public GetPackMessage(int id) {
        this.id = id;
    }
}