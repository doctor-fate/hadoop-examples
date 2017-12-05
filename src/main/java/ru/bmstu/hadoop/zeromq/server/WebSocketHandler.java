package ru.bmstu.hadoop.zeromq.server;

import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import org.zeromq.ZMQ;

public class WebSocketHandler extends AbstractReceiveListener {
    private final static ThreadLocal<ZMQ.Socket> socket = new ThreadLocal<>();

    WebSocketHandler(final ZMQ.Context context) {
        if (socket.get() == null) {
            ZMQ.Socket s = context.socket(ZMQ.PUSH);
            s.connect("inproc://messaging");
            socket.set(s);
        }
    }

    @Override
    protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) {
        socket.get().send(message.getData());
    }
}