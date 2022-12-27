package io.deepstream.protobuf;

import io.deepstream.protobuf.util.HexConverter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventMessageTest {
    @Test
    void subscribe() throws Exception {
        String input = "1610041a1208031a01303a0b646174612f6368616e6765";
        var data = HexConverter.convertFromHex(input);
        var message = General.Message.parseDelimitedFrom(new ByteArrayInputStream(data));
        assertEquals(General.TOPIC.EVENT, message.getTopic());

        Event.EventMessage eventMessage = Event.EventMessage.parseFrom(message.getMessage());
        System.out.println(eventMessage);
        assertEquals(Event.EVENT_ACTION.EVENT_SUBSCRIBE, eventMessage.getAction());
        assertEquals("data/change", eventMessage.getNames(0));

        eventMessage = Event.EventMessage.newBuilder()
                .setAction(Event.EVENT_ACTION.EVENT_SUBSCRIBE)
                .addNames("data/change")
                .setCorrelationId("0")
                .build();
        message = General.Message.newBuilder()
                .setTopic(General.TOPIC.EVENT)
                .setMessage(eventMessage.toByteString())
                .build();
        assertEquals(input, HexConverter.convertToHexString(message));
    }

    @Test
    void subscribeResponse() throws Exception {
        var data = HexConverter.convertFromHex("0b10041a0708031a01302801");
        var message = General.Message.parseDelimitedFrom(new ByteArrayInputStream(data));
        assertEquals(General.TOPIC.EVENT, message.getTopic());
        Event.EventMessage eventMessage = Event.EventMessage.parseFrom(message.getMessage());
        System.out.println(eventMessage);
        assertEquals(Event.EVENT_ACTION.EVENT_SUBSCRIBE, eventMessage.getAction());
        assertTrue(eventMessage.getIsAck());
    }

    @Test
    void emit() throws Exception {
        String input = "1b10041a1708021206227465737422320b646174612f6368616e6765";
        var data = HexConverter.convertFromHex(input);
        var message = General.Message.parseDelimitedFrom(new ByteArrayInputStream(data));
        assertEquals(General.TOPIC.EVENT, message.getTopic());

        Event.EventMessage eventMessage = Event.EventMessage.parseFrom(message.getMessage());
        System.out.println(eventMessage);
        assertEquals(Event.EVENT_ACTION.EVENT_EMIT, eventMessage.getAction());
        assertEquals("\"test\"", eventMessage.getData());
        assertEquals("data/change", eventMessage.getName());

        eventMessage = Event.EventMessage.newBuilder()
                .setAction(Event.EVENT_ACTION.EVENT_EMIT)
                // data must be in json format
                .setData("\"test\"")
                .setName("data/change")
                .build();
        message = General.Message.newBuilder()
                .setTopic(General.TOPIC.EVENT)
                .setMessage(eventMessage.toByteString())
                .build();
        assertEquals(input, HexConverter.convertToHexString(message));
    }
}
