package ru.bmstu.hadoop.zeromq.server;

import io.undertow.websockets.WebSocketProtocolHandshakeHandler;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import org.zeromq.ZMQ;

import java.util.concurrent.atomic.AtomicBoolean;

public class SubscriberThread extends Thread {
    private final WebSocketProtocolHandshakeHandler handler;
    private final ZMQ.Socket socket;
    private final AtomicBoolean exit = new AtomicBoolean(false);

    SubscriberThread(ZMQ.Context context, String address, WebSocketProtocolHandshakeHandler handler) {
        this.handler = handler;

        socket = context.socket(ZMQ.SUB);
        socket.connect(address);
        socket.subscribe("");
    }

    public void shutdown() {
        exit.set(true);
    }

    public void run() {
        while (!exit.get()) {
            final String data = socket.recvStr(ZMQ.DONTWAIT);
            if (data != null) {
                for (WebSocketChannel session : handler.getPeerConnections()) {
                    WebSockets.sendText(data, session, null);
                }
            }
        }
    }
}