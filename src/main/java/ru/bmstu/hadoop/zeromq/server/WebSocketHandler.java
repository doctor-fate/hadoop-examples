package ru.bmstu.hadoop.zeromq.server;

import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import org.zeromq.ZMQ;

public class WebSocketHandler extends AbstractReceiveListener {
    private final static ThreadLocal<ZMQ.Socket> socket = new ThreadLocal<>();
    private final ZMQ.Context context;
    private final String address;

    WebSocketHandler(ZMQ.Context context, String address) {
        this.context = context;
        this.address = address;
    }

    @Override
    protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) {
        ZMQ.Socket push = socket.get();
        if (push == null) {
            push = context.socket(ZMQ.PUSH);
            push.connect(address);
            socket.set(push);
        }
        push.send(message.getData());
    }
}