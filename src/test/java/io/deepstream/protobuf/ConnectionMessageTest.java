package io.deepstream.protobuf;

import io.deepstream.protobuf.util.HexConverter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectionMessageTest {
    @Test
    void connectionChallenge() throws IOException {
        String input = "3f10021a3b08052a1e77733a2f2f3132372e302e302e313a363032302f6465657073747265616d3204302e31613a05362e302e35420a6a617661736372697074";
        byte[] data = HexConverter.convertFromHex(input);

        General.Message message = General.Message.parseDelimitedFrom(new ByteArrayInputStream(data));
        assertEquals(General.TOPIC.CONNECTION, message.getTopic());
        Connection.ConnectionMessage connectionMessage = Connection.ConnectionMessage.parseFrom(message.getMessage());
        System.out.println(connectionMessage);
        assertEquals(Connection.CONNECTION_ACTION.CONNECTION_CHALLENGE, connectionMessage.getAction());
        assertEquals("ws://127.0.0.1:6020/deepstream", connectionMessage.getUrl());
        assertEquals("0.1a", connectionMessage.getProtocolVersion());
        assertEquals("6.0.5", connectionMessage.getSdkVersion());
        assertEquals("javascript", connectionMessage.getSdkType());

        connectionMessage = Connection.ConnectionMessage.newBuilder()
                .setAction(Connection.CONNECTION_ACTION.CONNECTION_CHALLENGE)
                .setUrl("ws://127.0.0.1:6020/deepstream")
                .setProtocolVersion("0.1a")
                .setSdkVersion("6.0.5")
                .setSdkType("javascript")
                .build();
        message = General.Message.newBuilder()
                .setTopic(General.TOPIC.CONNECTION)
                .setMessage(connectionMessage.toByteString())
                .build();
        String hexString = HexConverter.convertToHexString(message);
        assertEquals(input, hexString);
    }

    @Test
    void challengeResponse() throws Exception {
        var data = HexConverter.convertFromHex("0610021a020804");
        var message = General.Message.parseDelimitedFrom(new ByteArrayInputStream(data));
        Connection.ConnectionMessage connectionMessage = Connection.ConnectionMessage.parseFrom(message.getMessage());
        System.out.println(connectionMessage);
        assertEquals(Connection.CONNECTION_ACTION.CONNECTION_ACCEPT, connectionMessage.getAction());
    }

    @Test
    void ping() throws Exception {
        String input = "0610021a020802";
        var data = HexConverter.convertFromHex(input);
        var message = General.Message.parseDelimitedFrom(new ByteArrayInputStream(data));
        Connection.ConnectionMessage connectionMessage = Connection.ConnectionMessage.parseFrom(message.getMessage());
        System.out.println(connectionMessage);
        assertEquals(Connection.CONNECTION_ACTION.CONNECTION_PING, connectionMessage.getAction());

        connectionMessage = Connection.ConnectionMessage.newBuilder()
                .setAction(Connection.CONNECTION_ACTION.CONNECTION_PING)
                .build();
        message = General.Message.newBuilder()
                .setTopic(General.TOPIC.CONNECTION)
                .setMessage(connectionMessage.toByteString())
                .build();
        String hexString = HexConverter.convertToHexString(message);
        assertEquals(input, hexString);
    }

    @Test
    void pong() throws Exception {
        var data = HexConverter.convertFromHex("0610021a020803");
        var message = General.Message.parseDelimitedFrom(new ByteArrayInputStream(data));
        Connection.ConnectionMessage connectionMessage = Connection.ConnectionMessage.parseFrom(message.getMessage());
        System.out.println(connectionMessage);
        assertEquals(Connection.CONNECTION_ACTION.CONNECTION_PONG, connectionMessage.getAction());
    }
}
