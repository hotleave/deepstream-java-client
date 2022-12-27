package io.github.hotleave.deepstream.client;

import jakarta.websocket.DeploymentException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;

@Slf4j
@Disabled
class ClientTest {
    @Test
    void testConnection() throws DeploymentException, URISyntaxException, IOException, InterruptedException {
        Client client = new Client("ws://127.0.0.1:6020/deepstream");
        client.login();

        client.event.subscribe("data/change", msg -> {
            log.info("Message from event listener: {}", msg);
        });
        client.event.emit("data/change", "\"hotleave\"");

        Thread.currentThread().join();
    }
}