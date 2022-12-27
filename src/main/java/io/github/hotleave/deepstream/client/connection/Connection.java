package io.github.hotleave.deepstream.client.connection;

import com.google.protobuf.GeneratedMessageV3;
import io.deepstream.protobuf.Auth;
import io.deepstream.protobuf.Connection.CONNECTION_ACTION;
import io.deepstream.protobuf.Connection.ConnectionMessage;
import io.deepstream.protobuf.General;
import io.github.hotleave.deepstream.client.utils.StateMachine;
import jakarta.websocket.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.deepstream.protobuf.Auth.AUTH_ACTION.*;
import static io.deepstream.protobuf.Connection.CONNECTION_ACTION.*;
import static io.github.hotleave.deepstream.client.connection.ConnectionState.*;

@ClientEndpoint
@Slf4j
public class Connection {
    private Session session;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final Map<General.TOPIC, Consumer<General.Message>> handlerMap = new HashMap<>();
    private final StateMachine stateMachine;

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicInteger retryCount = new AtomicInteger();
    private ScheduledFuture<?> heartbeatScheduledFuture;
    private URI uri;

    public Connection(String url) throws URISyntaxException {
        List<StateMachine.Transition> transitions = List.of(
                new StateMachine.Transition("initialised", CLOSED, INITIALISING),
                new StateMachine.Transition("connected", INITIALISING, AWAITING_CONNECTION),
                new StateMachine.Transition("connected", REDIRECTING, AWAITING_CONNECTION),
                new StateMachine.Transition("connected", RECONNECTING, AWAITING_CONNECTION),
                new StateMachine.Transition("challenge", AWAITING_CONNECTION, CHALLENGING),
                new StateMachine.Transition("redirected", CHALLENGING, REDIRECTING),
                new StateMachine.Transition("challenge-denied", CHALLENGING, CHALLENGE_DENIED),
                new StateMachine.Transition("accepted", CHALLENGING, AWAITING_AUTHENTICATION, this::onAwaitingAuthentication),
                new StateMachine.Transition("authentication-timeout", AWAITING_CONNECTION, AUTHENTICATION_TIMEOUT),
                new StateMachine.Transition("authentication-timeout", AWAITING_AUTHENTICATION, AUTHENTICATION_TIMEOUT),
                new StateMachine.Transition("authenticate", AWAITING_AUTHENTICATION, AUTHENTICATING),
                new StateMachine.Transition("unsuccessful-login", AUTHENTICATING, AWAITING_AUTHENTICATION),
                new StateMachine.Transition("successful-login", AUTHENTICATING, OPEN),
                new StateMachine.Transition("too-many-auth-attempts", AUTHENTICATING, TOO_MANY_AUTH_ATTEMPTS),
                new StateMachine.Transition("too-many-auth-attempts", AWAITING_AUTHENTICATION, TOO_MANY_AUTH_ATTEMPTS),
                new StateMachine.Transition("authentication-timeout", AWAITING_AUTHENTICATION, AUTHENTICATION_TIMEOUT),
                new StateMachine.Transition("reconnect", RECONNECTING, RECONNECTING),
                new StateMachine.Transition("closed", CLOSING, CLOSED),
                new StateMachine.Transition("offline", PAUSING, OFFLINE),
                new StateMachine.Transition("error", null, RECONNECTING),
                new StateMachine.Transition("connection-lost", null, RECONNECTING),
                new StateMachine.Transition("resume", null, RECONNECTING),
                new StateMachine.Transition("pause", null, PAUSING),
                new StateMachine.Transition("close", null, CLOSING)
        );

        stateMachine = new StateMachine(transitions, this::onStateChange);
        stateMachine.transition("initialised");

        this.uri = new URI(url);
    }

    public void connect() throws DeploymentException, IOException {
        synchronized (this) {
            if (connected.get()) {
                log.info("Connected, abort");
                return;
            }

            WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            session = container.connectToServer(this, uri);

            while (!connected.get()) {
                log.debug("Connection is not ready, wait for it ready...");
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            log.info("Connected, ready for data transfer.");
        }
    }

    public void registerHandler(General.TOPIC topic, Consumer<General.Message> handler) {
        handlerMap.put(topic, handler);
    }

    public boolean isConnected() {
        return stateMachine.getState() == OPEN;
    }

    public ConnectionState getConnectionState() {
        return stateMachine.getState();
    }

    public void close() {
        if (heartbeatScheduledFuture != null) {
            heartbeatScheduledFuture.cancel(true);
        }

        ConnectionMessage message = ConnectionMessage.newBuilder()
                .setAction(CONNECTION_ACTION.CONNECTION_CLOSING)
                .build();
        sendMessage(General.TOPIC.CONNECTION, message);
        stateMachine.transition("close");
    }

    @OnOpen
    public void onOpen(Session session) {
        this.session = session;

        stateMachine.transition("connected");

        // 定时发送ping
        heartbeatScheduledFuture = scheduledExecutorService.scheduleAtFixedRate(this::ping, 5, 5, TimeUnit.SECONDS);

        // 发送challenge
        ConnectionMessage challenge = ConnectionMessage.newBuilder()
                .setAction(CONNECTION_ACTION.CONNECTION_CHALLENGE)
                .setUrl(uri.toString())
                .setProtocolVersion("0.1a")
                .setSdkVersion("1.0.5")
                .setSdkType("java")
                .build();
        sendMessage(General.TOPIC.CONNECTION, challenge);

        stateMachine.transition("challenge");
    }

    @OnError
    public void onError(Throwable throwable) {
        log.error("Connection error: {}", throwable.getMessage(), throwable);

        stateMachine.transition("error");

        if (heartbeatScheduledFuture != null) {
            heartbeatScheduledFuture.cancel(true);
        }

        tryReconnect();
    }

    private void tryReconnect() {
        if (connected.get()) {
            return;
        }

        if (retryCount.incrementAndGet() >= 5) {
            log.info("Connect to {} failed after {} times retry", uri, retryCount);
            return;
        }

        try {
            connect();
        } catch (DeploymentException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @OnClose
    public void onClose() {
        log.debug("Websocket closed");
        if (heartbeatScheduledFuture != null) {
            heartbeatScheduledFuture.cancel(true);
        }

        if (stateMachine.getState() == CHALLENGE_DENIED ||
                stateMachine.getState() == TOO_MANY_AUTH_ATTEMPTS ||
                stateMachine.getState() == AUTHENTICATION_TIMEOUT) {
            return;
        }

        if (stateMachine.getState() == CLOSING) {
            this.stateMachine.transition("closed");
            return;
        }
        if (stateMachine.getState() == PAUSING) {
            this.stateMachine.transition("offline");
            return;
        }
        stateMachine.transition("connection-lost");
        tryReconnect();
    }

    @OnMessage
    @SneakyThrows(IOException.class)
    public void onMessage(byte[] message) {
        General.Message msg = General.Message.parseDelimitedFrom(new ByteArrayInputStream(message));
        log.debug("topic = {}", msg.getTopic());

        if (msg.getTopic() == General.TOPIC.CONNECTION) {
            var connectionMessage = ConnectionMessage.parseFrom(msg.getMessage());
            log.debug("Connection response {}", connectionMessage);
            handleConnectionResponse(connectionMessage);
        } else if (msg.getTopic() == General.TOPIC.AUTH) {
            var authMessage = Auth.AuthMessage.parseFrom(msg.getMessage());
            log.info("Auth response: {}", authMessage);
            handleAuthResponse(authMessage);
        } else {
            Consumer<General.Message> handler = handlerMap.get(msg.getTopic());
            handler.accept(msg);
        }
    }

    private void handleAuthResponse(Auth.AuthMessage message) {
        if (message.getAction() == AUTH_TOO_MANY_AUTH_ATTEMPTS) {
            stateMachine.transition("too-many-auth-attempts");
            log.error("Too many auth attempts: {}", message);
        } else if (message.getAction() == AUTH_AUTH_UNSUCCESSFUL) {
            stateMachine.transition("unsuccessful-login");
            log.info("Auth unsuccessful: Invalid auth details");
        } else if (message.getAction() == AUTH_AUTH_SUCCESSFUL) {
            stateMachine.transition("successful-login");
            onAuthSuccessful(message.getData());
        }
    }

    private void onAuthSuccessful(String data) {
        synchronized (this) {
            log.debug("Client data is: {}", data);

            this.connected.set(true);
            this.notifyAll();
        }
    }

    @SneakyThrows(URISyntaxException.class)
    private void handleConnectionResponse(ConnectionMessage message) {
        if (message.getAction() == CONNECTION_ACCEPT) {
            stateMachine.transition("accepted");
            return;
        }
        if (message.getAction() == CONNECTION_REJECT) {
            stateMachine.transition("challenge-denied");
            closeSession();
            return;
        }
        if (message.getAction() == CONNECTION_REDIRECT) {
            uri = new URI(message.getUrl());
            stateMachine.transition("redirected");
            closeSession();
            return;
        }
        if (message.getAction() == CONNECTION_AUTHENTICATION_TIMEOUT) {
            this.stateMachine.transition("authentication-timeout");
            log.error("Authentication timeout: {}", message);
        }
    }

    @SneakyThrows(IOException.class)
    private void closeSession() {
        if (session != null) {
            session.close();
        }
    }

    private void onStateChange(ConnectionState newState, ConnectionState oldState) {
        if (newState == oldState) {
            return;
        }
    }

    private void onAwaitingAuthentication() {
        stateMachine.transition("authenticate");

        Auth.AuthMessage authMessage = Auth.AuthMessage.newBuilder()
                .setAction(Auth.AUTH_ACTION.AUTH_REQUEST)
                .setData("{}")
                .build();
        log.debug("Send auth message: {}", authMessage);
        sendMessage(General.TOPIC.AUTH, authMessage);
    }

    private void ping() {
        ConnectionMessage ping = ConnectionMessage.newBuilder()
                .setAction(CONNECTION_ACTION.CONNECTION_PING)
                .build();
        log.debug("Send ping message: {}", ping);

        sendMessage(General.TOPIC.CONNECTION, ping);
    }

    @SneakyThrows(IOException.class)
    public void sendMessage(General.TOPIC topic, GeneratedMessageV3 msg) {
        General.Message message = General.Message.newBuilder()
                .setTopic(topic)
                .setMessage(msg.toByteString())
                .build();

        var outputStream = new ByteArrayOutputStream();
        message.writeDelimitedTo(outputStream);
        var buffer = ByteBuffer.wrap(outputStream.toByteArray());
        session.getAsyncRemote().sendBinary(buffer);
    }
}
