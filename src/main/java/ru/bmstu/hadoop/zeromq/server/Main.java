package ru.bmstu.hadoop.zeromq.server;

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.websockets.WebSocketProtocolHandshakeHandler;
import org.zeromq.ZMQ;

import java.io.IOException;


public class Main {
    public static void main(final String[] args) throws IOException {
        final ZMQ.Context context = ZMQ.context(1);

        WebSocketProtocolHandshakeHandler websocket = Handlers.websocket((exchange, channel) -> {
            channel.getReceiveSetter().set(new WebSocketHandler(context, args[0]));
            channel.resumeReceives();
        });

        ResourceHandler resources = Handlers.resource(new ClassPathResourceManager(Main.class.getClassLoader(), ""))
                .addWelcomeFiles("index.html");

        int port = Integer.parseInt(args[0].split(":")[2]) + 1000;
        Undertow server = Undertow.builder()
                .addHttpListener(port, "127.0.0.1")
                .setHandler(Handlers.path().addPrefixPath("/chatsocket", websocket).addPrefixPath("/", resources))
                .build();

        SubscriberThread subscriber = new SubscriberThread(context, args[2], websocket);
        subscriber.start();

        new Thread(() -> {
            ZMQ.Socket frontend = context.socket(ZMQ.PULL);
            frontend.bind(args[0]);
            ZMQ.Socket backend = context.socket(ZMQ.PUB);
            backend.connect(args[1]);

            ZMQ.proxy(frontend, backend, null);

            frontend.close();
            backend.close();
        }).start();

        new Thread(server::start).start();

        System.out.printf("Server online at http://127.0.0.1:%d\nPress RETURN to stop...\n", port);

        //noinspection ResultOfMethodCallIgnored
        System.in.read();

        server.stop();

        subscriber.shutdown();

        context.term(); //never returns
    }
}
