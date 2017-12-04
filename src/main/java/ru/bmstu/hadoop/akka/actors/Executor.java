package ru.bmstu.hadoop.akka.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import ru.bmstu.hadoop.akka.messages.ExecuteTestMessage;
import ru.bmstu.hadoop.akka.messages.StoreResultMessage;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Executor extends AbstractActor {
    private static final ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
    private final ActorRef repository;

    Executor(ActorRef repository) {
        this.repository = repository;
    }

    public static Props props(ActorRef repository) {
        return Props.create(Executor.class, repository);
    }

    private static String execute(ExecuteTestMessage message) throws ScriptException, NoSuchMethodException {
        engine.eval(message.script);
        Invocable invocable = (Invocable) engine;
        return invocable.invokeFunction(message.function, message.test.parameters).toString();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().
                match(ExecuteTestMessage.class, message -> {
                    String result = execute(message);
                    repository.tell(new StoreResultMessage(message, result), getSelf());
                }).
                build();
    }
}