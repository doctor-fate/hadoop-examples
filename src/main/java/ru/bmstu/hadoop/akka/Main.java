package ru.bmstu.hadoop.akka;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.unmarshalling.StringUnmarshallers;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.util.Timeout;
import ru.bmstu.hadoop.akka.actors.Router;
import ru.bmstu.hadoop.akka.messages.ExecutePackMessage;
import ru.bmstu.hadoop.akka.messages.GetPackMessage;
import ru.bmstu.hadoop.akka.models.Pack;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static akka.pattern.PatternsCS.ask;


public class Main extends AllDirectives {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 10789;
    private final ActorRef router;

    private Main(final ActorSystem system) {
        router = system.actorOf(Router.props(), "router");
    }

    public static void main(String[] args) throws Exception {
        ActorSystem system = ActorSystem.create("routes");

        final Http http = Http.get(system);
        final ActorMaterializer materializer = ActorMaterializer.create(system);

        Main application = new Main(system);

        final Flow<HttpRequest, HttpResponse, NotUsed> flow = application.createRoute().flow(system, materializer);
        final CompletionStage<ServerBinding> binding = http.bindAndHandle(flow, ConnectHttp.toHost(HOST, PORT), materializer);

        System.out.printf("Server online at http://%s:%s/\nPress RETURN to stop...", HOST, PORT);

        //noinspection ResultOfMethodCallIgnored
        System.in.read();

        binding.thenCompose(ServerBinding::unbind).thenAccept(unbound -> system.terminate());
    }

    private Route createRoute() {
        return route(
                path("results", () -> route(
                        get(() ->
                                parameter(StringUnmarshallers.INTEGER, "id", id -> {
                                            final Timeout timeout = Timeout.durationToTimeout(FiniteDuration.apply(8, TimeUnit.SECONDS));
                                            CompletionStage<Pack> results = ask(router, new GetPackMessage(id), timeout).
                                                    thenApply(Pack.class::cast);
                                            return completeOKWithFuture(results, Jackson.marshaller());
                                        }
                                )))),
                path("execute", () -> route(
                        post(() ->
                                entity(Jackson.unmarshaller(ExecutePackMessage.class), message -> {
                                    router.tell(message, ActorRef.noSender());
                                            return complete("OK");
                                        }
                                ))))
        );
    }
}