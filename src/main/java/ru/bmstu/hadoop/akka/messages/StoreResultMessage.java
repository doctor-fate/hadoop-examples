package ru.bmstu.hadoop.akka.messages;

import ru.bmstu.hadoop.akka.models.Pack;

public class StoreResultMessage {
    public final int id;
    public final Pack.Result result;

    public StoreResultMessage(int id, Pack.Result result) {
        this.id = id;
        this.result = result;
    }
}