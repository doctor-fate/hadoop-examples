package ru.bmstu.hadoop.akka.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import ru.bmstu.hadoop.akka.messages.GetPackMessage;
import ru.bmstu.hadoop.akka.messages.StoreResultMessage;
import ru.bmstu.hadoop.akka.models.Pack;

import java.util.HashMap;
import java.util.Map;

public class Repository extends AbstractActor {
    private final Map<Integer, Pack> results = new HashMap<>();

    public static Props props() {
        return Props.create(Repository.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().
                match(GetPackMessage.class,
                        message -> sender().tell(getPack(message.id), self())).
                match(StoreResultMessage.class,
                        message -> storeResult(message.id, message.result)).
                build();
    }

    private Pack getPack(int id) {
        return results.getOrDefault(id, new Pack());
    }

    private void storeResult(int id, Pack.Result e) {
        Pack pack = getPack(id);
        pack.getResults().add(e);
        results.putIfAbsent(id, pack);
    }
}