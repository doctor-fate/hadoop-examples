package ru.bmstu.hadoop.akka.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.routing.RoundRobinPool;
import ru.bmstu.hadoop.akka.messages.ExecutePackMessage;
import ru.bmstu.hadoop.akka.messages.ExecuteTestMessage;
import ru.bmstu.hadoop.akka.messages.GetPackMessage;

public class Router extends AbstractActor {
    private final ActorRef repository = getContext().actorOf(Repository.props(), "repository");
    private final ActorRef executors = getContext().actorOf(new RoundRobinPool(4).props(Executor.props(repository)), "executors");

    public static Props props() {
        return Props.create(Router.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().
                match(GetPackMessage.class, message -> repository.forward(message, getContext())).
                match(ExecutePackMessage.class, message -> {
                    for (ExecutePackMessage.Test test : message.tests) {
                        executors.forward(new ExecuteTestMessage(message, test), getContext());
                    }
                }).
                build();
    }
}