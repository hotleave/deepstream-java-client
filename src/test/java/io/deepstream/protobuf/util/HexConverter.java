package io.deepstream.protobuf.util;

import io.deepstream.protobuf.General;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class HexConverter {
    private static final char[] HEX_CHARS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    public static byte[] convertFromHex(String input) {
        char[] chars = input.toCharArray();
        if (chars.length % 2 != 0) {
            throw new IllegalArgumentException("Input length must be even");
        }

        byte[] result = new byte[chars.length / 2];
        for (int i = 0; i < result.length; i++) {
            int index = i << 1;
            byte b = (byte) (toByte(chars[index]) << 4 | toByte(chars[index + 1]));
            result[i] = b;
        }

        return result;
    }

    public static String convertToHexString(byte[] input) {
        char[] result = new char[input.length * 2];
        for (int i = 0; i < input.length; i++) {
            byte b = input[i];
            result[i * 2] = HEX_CHARS[b >> 4 & 0x0F];
            result[i * 2 + 1] = HEX_CHARS[b & 0x0F];
        }

        return new String(result);
    }

    public static String convertToHexString(General.Message message) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        message.writeDelimitedTo(outputStream);
        return HexConverter.convertToHexString(outputStream.toByteArray());
    }

    private static byte toByte(char c) {
        int digit = Character.digit(c, 16);
        if (digit >= 0 && digit <= 15) {
            return (byte) digit;
        }

        throw new IllegalArgumentException("Invalid hex character: " + c);
    }
}
