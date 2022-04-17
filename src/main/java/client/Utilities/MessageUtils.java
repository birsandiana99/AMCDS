package client.Utilities;

import com.google.common.primitives.Ints;
import main.CommunicationProtocol.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;

public class MessageUtils {
    public static void write(OutputStream out, Message m) throws IOException {
        byte[] bytes = m.toByteArray();
        out.write(Ints.toByteArray(bytes.length), 0, Integer.SIZE/Byte.SIZE);
        out.write(bytes, 0, bytes.length);
    }

    public static Message read(InputStream in) throws IOException {
        byte[] lengthMessage = new byte[Integer.SIZE/Byte.SIZE];
        in.read(lengthMessage, 0, Integer.SIZE/Byte.SIZE);
        byte[] message = new byte[new BigInteger(lengthMessage).intValue()];
        in.read(message, 0, new BigInteger(lengthMessage).intValue());
        return Message.parseFrom(message);
    }
}
