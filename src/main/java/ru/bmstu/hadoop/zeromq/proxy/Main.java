package ru.bmstu.hadoop.zeromq.proxy;

import org.zeromq.ZMQ;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        ZMQ.Context context = ZMQ.context(1);
        Thread proxy = new Thread(() -> {
            ZMQ.Socket frontend = context.socket(ZMQ.XSUB);
            frontend.bind(args[0]);
            ZMQ.Socket backend = context.socket(ZMQ.XPUB);
            backend.bind(args[1]);

            ZMQ.proxy(frontend, backend, null);

            frontend.close();
            backend.close();
        });
        proxy.start();

        System.out.printf("Proxy online at %s <--> %s\nPress RETURN to stop...", args[0], args[1]);

        //noinspection ResultOfMethodCallIgnored
        System.in.read();

        context.term();
        proxy.join();
    }
}