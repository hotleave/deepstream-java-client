package io.deepstream.protobuf;

import io.deepstream.protobuf.util.HexConverter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AuthMessageTest {
    @Test
    void sendAuthParams() throws Exception {
        String input = "0a10031a06080212027b7d";
        var data = HexConverter.convertFromHex(input);
        var message = General.Message.parseDelimitedFrom(new ByteArrayInputStream(data));
        assertEquals(General.TOPIC.AUTH, message.getTopic());

        Auth.AuthMessage authMessage = Auth.AuthMessage.parseFrom(message.getMessage());
        System.out.println(authMessage);
        assertEquals(Auth.AUTH_ACTION.AUTH_REQUEST, authMessage.getAction());
        assertEquals("{}", authMessage.getData());

        authMessage = Auth.AuthMessage.newBuilder()
                .setAction(Auth.AUTH_ACTION.AUTH_REQUEST)
                .setData("{}")
                .build();
        message = General.Message.newBuilder()
                .setTopic(General.TOPIC.AUTH)
                .setMessage(authMessage.toByteString())
                .build();
        assertEquals(input, HexConverter.convertToHexString(message));
    }

    @Test
    void authResponse() throws Exception {
        String input = "0610031a020803";
        var data = HexConverter.convertFromHex(input);
        var message = General.Message.parseDelimitedFrom(new ByteArrayInputStream(data));
        assertEquals(General.TOPIC.AUTH, message.getTopic());

        Auth.AuthMessage authMessage = Auth.AuthMessage.parseFrom(message.getMessage());
        System.out.println(authMessage);
        assertEquals(Auth.AUTH_ACTION.AUTH_AUTH_SUCCESSFUL, authMessage.getAction());
    }
}
