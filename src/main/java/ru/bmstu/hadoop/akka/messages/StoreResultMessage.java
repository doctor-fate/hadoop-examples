package ru.bmstu.hadoop.akka.messages;

import ru.bmstu.hadoop.akka.models.Pack;

public class StoreResultMessage {
    public final int id;
    public final Pack.Result result;

    public StoreResultMessage(ExecuteTestMessage message, String result) {
        this.id = message.id;
        this.result = new Pack.Result(message.test, result);
    }
}