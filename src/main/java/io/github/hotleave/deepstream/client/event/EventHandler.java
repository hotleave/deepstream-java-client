package io.github.hotleave.deepstream.client.event;

import com.google.protobuf.InvalidProtocolBufferException;
import io.deepstream.protobuf.Event;
import io.deepstream.protobuf.General;
import io.github.hotleave.deepstream.client.connection.Connection;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Slf4j
public class EventHandler {
    Connection connection;
    Map<String, Consumer<String>> eventHandlerMap = new ConcurrentHashMap<>();

    public void subscribe(String name, Consumer<String> callback) {
        Event.EventMessage msg = Event.EventMessage.newBuilder()
                .setAction(Event.EVENT_ACTION.EVENT_SUBSCRIBE)
                .setCorrelationId(UUID.randomUUID().toString())
                .addNames(name)
                .build();
        connection.sendMessage(General.TOPIC.EVENT, msg);

        eventHandlerMap.put(name, callback);
    }

    public void unsubscribe(String name) {
        Event.EventMessage msg = Event.EventMessage.newBuilder()
                .setAction(Event.EVENT_ACTION.EVENT_UNSUBSCRIBE)
                .setCorrelationId(UUID.randomUUID().toString())
                .addNames(name)
                .build();
        connection.sendMessage(General.TOPIC.EVENT, msg);

        eventHandlerMap.remove(name);
    }

    public void emit(String name, String data) {
        Event.EventMessage msg = Event.EventMessage.newBuilder()
                .setAction(Event.EVENT_ACTION.EVENT_EMIT)
                .setCorrelationId(UUID.randomUUID().toString())
                .setName(name)
                .setData(data)
                .build();
        connection.sendMessage(General.TOPIC.EVENT, msg);
    }

    public void handle(General.Message message) {
        try {
            Event.EventMessage eventMessage = Event.EventMessage.parseFrom(message.getMessage());
            if (eventMessage.getIsAck()) {
                return;
            }

            String name = eventMessage.getName();
            switch (eventMessage.getAction()) {
                case EVENT_EMIT:
                case EVENT_SUBSCRIPTION_FOR_PATTERN_FOUND:
                    Consumer<String> handler = eventHandlerMap.get(name);
                    if (handler == null) {
                        log.warn("Event handler for {} not found!", name);
                    } else {
                        handler.accept(eventMessage.getData());
                    }
                    break;
                case EVENT_MESSAGE_DENIED:
                case EVENT_SUBSCRIPTION_FOR_PATTERN_REMOVED:
                    log.error("Event message denied or removed: {}", eventMessage);
                    if (eventMessage.getOriginalAction() == Event.EVENT_ACTION.EVENT_SUBSCRIBE) {
                        eventHandlerMap.remove(name);
                    }
                    break;
                case EVENT_MULTIPLE_SUBSCRIPTIONS:
                case EVENT_NOT_SUBSCRIBED:
                    log.warn("Event failed: {}", eventMessage);
                    break;
                case EVENT_INVALID_LISTEN_REGEX:
                    log.error("Invalid listen regex: {}", eventMessage);
                    break;
                default:
                    log.error("Unsupported event action: {}", eventMessage);
                    break;
            }
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
