package io.github.hotleave.deepstream.client;

import io.deepstream.protobuf.General;
import io.github.hotleave.deepstream.client.connection.Connection;
import io.github.hotleave.deepstream.client.event.EventHandler;
import jakarta.websocket.DeploymentException;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;

import java.io.IOException;
import java.net.URISyntaxException;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class Client {
    Connection connection;

    public final EventHandler event;

    public Client(String url) throws URISyntaxException {
        connection = new Connection(url);
        event = new EventHandler(connection);
        connection.registerHandler(General.TOPIC.EVENT, event::handle);
    }

    public void login() throws DeploymentException, IOException {
        connection.connect();
    }
}
