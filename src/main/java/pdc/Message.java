package pdc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Message represents a custom binary protocol frame used in CSM218.
 *
 * Wire Format (Big Endian):
 *
 * | int MAGIC_LEN | MAGIC bytes |
 * | int VERSION |
 * | int TYPE_LEN | TYPE bytes |
 * | int SENDER_LEN | SENDER bytes |
 * | int STUDENT_LEN | STUDENT bytes |
 * | long TIMESTAMP |
 * | int PAYLOAD_LEN | PAYLOAD bytes |
 *
 * Design Notes:
 * - Length-prefixed framing prevents TCP stream corruption
 * - No JSON or Java Serialization used
 * - Defensive validation protects against malformed frames
 */
public class Message {

    public static final String EXPECTED_MAGIC = "CSM218";

    // Required by autograder
    public String messageType;
    public String studentId = "N02220159P";

    // Core protocol fields
    public String magic;
    public int version;
    public String sender;
    public long timestamp;
    public byte[] payload;

    public Message() {}

    /**
     * Serializes the message into a binary frame.
     */
    public byte[] pack() {

        if (magic == null || messageType == null || sender == null || studentId == null) {
            throw new IllegalStateException("Required message fields missing.");
        }

        byte[] magicBytes   = magic.getBytes(StandardCharsets.UTF_8);
        byte[] typeBytes    = messageType.getBytes(StandardCharsets.UTF_8);
        byte[] senderBytes  = sender.getBytes(StandardCharsets.UTF_8);
        byte[] studentBytes = studentId.getBytes(StandardCharsets.UTF_8);
        byte[] payloadBytes = (payload == null) ? new byte[0] : payload;

        int totalSize =
                4 + magicBytes.length +
                        4 +
                        4 + typeBytes.length +
                        4 + senderBytes.length +
                        4 + studentBytes.length +
                        8 +
                        4 + payloadBytes.length;

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.order(ByteOrder.BIG_ENDIAN);

        // MAGIC
        buffer.putInt(magicBytes.length);
        buffer.put(magicBytes);

        // VERSION
        buffer.putInt(version);

        // MESSAGE TYPE
        buffer.putInt(typeBytes.length);
        buffer.put(typeBytes);

        // SENDER
        buffer.putInt(senderBytes.length);
        buffer.put(senderBytes);

        // STUDENT ID
        buffer.putInt(studentBytes.length);
        buffer.put(studentBytes);

        // TIMESTAMP
        buffer.putLong(timestamp);

        // PAYLOAD
        buffer.putInt(payloadBytes.length);
        buffer.put(payloadBytes);

        return buffer.array();
    }

    /**
     * Deserializes a binary frame into a Message object.
     */
    public static Message unpack(byte[] data) {

        if (data == null || data.length < 20) {
            throw new IllegalArgumentException("Frame too small.");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.BIG_ENDIAN);

        Message message = new Message();

        // MAGIC
        int magicLen = buffer.getInt();
        validateLength(magicLen, buffer.remaining(), "magic");
        byte[] magicBytes = new byte[magicLen];
        buffer.get(magicBytes);
        message.magic = new String(magicBytes, StandardCharsets.UTF_8);

        if (!EXPECTED_MAGIC.equals(message.magic)) {
            throw new IllegalArgumentException("Invalid protocol magic.");
        }

        // VERSION
        message.version = buffer.getInt();

        // MESSAGE TYPE
        int typeLen = buffer.getInt();
        validateLength(typeLen, buffer.remaining(), "messageType");
        byte[] typeBytes = new byte[typeLen];
        buffer.get(typeBytes);
        message.messageType = new String(typeBytes, StandardCharsets.UTF_8);

        // SENDER
        int senderLen = buffer.getInt();
        validateLength(senderLen, buffer.remaining(), "sender");
        byte[] senderBytes = new byte[senderLen];
        buffer.get(senderBytes);
        message.sender = new String(senderBytes, StandardCharsets.UTF_8);

        // STUDENT ID
        int studentLen = buffer.getInt();
        validateLength(studentLen, buffer.remaining(), "studentId");
        byte[] studentBytes = new byte[studentLen];
        buffer.get(studentBytes);
        message.studentId = new String(studentBytes, StandardCharsets.UTF_8);

        // TIMESTAMP
        message.timestamp = buffer.getLong();

        // PAYLOAD
        int payloadLen = buffer.getInt();
        validateLength(payloadLen, buffer.remaining(), "payload");
        byte[] payloadBytes = new byte[payloadLen];
        buffer.get(payloadBytes);
        message.payload = payloadBytes;

        return message;
    }

    /**
     * Defensive validation to prevent malformed frame exploits.
     */
    private static void validateLength(int declared, int remaining, String field) {
        if (declared < 0 || declared > remaining) {
            throw new IllegalArgumentException("Invalid " + field + " length: " + declared);
        }
    }
}
