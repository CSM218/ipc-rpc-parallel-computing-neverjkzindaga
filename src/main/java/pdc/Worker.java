package pdc;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Worker node responsible for:
 * - Joining cluster
 * - Maintaining heartbeat
 * - Executing matrix block tasks
 */
public class Worker {

    private static final Logger logger = Logger.getLogger(Worker.class.getName());

    private static final String PROTOCOL_MAGIC = "CSM218";
    private static final int PROTOCOL_VERSION = 1;

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    private final Object writeLock = new Object();
    private final String workerId = UUID.randomUUID().toString();

    private final ExecutorService workerPool =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private final ScheduledExecutorService heartbeatScheduler =
            Executors.newSingleThreadScheduledExecutor();

    /**
     * Connects worker to Master.
     */
    public void joinCluster(String host, int port) {

        try {
            socket = new Socket(host, port);
            socket.setTcpNoDelay(true);

            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());

            sendRegister();
            startHeartbeat();
            listenForMessages();

            logger.info("Worker joined cluster successfully.");

        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to join cluster", e);
        }
    }

    private void sendRegister() throws IOException {

        Message msg = new Message();
        msg.magic = PROTOCOL_MAGIC;
        msg.version = PROTOCOL_VERSION;
        msg.messageType = "REGISTER";
        msg.sender = workerId;
        msg.timestamp = System.currentTimeMillis();
        msg.payload = "CAPABILITY:CPU".getBytes(StandardCharsets.UTF_8);

        sendMessage(msg);
    }

    private void startHeartbeat() {

        heartbeatScheduler.scheduleAtFixedRate(() -> {

            try {
                Message hb = new Message();
                hb.magic = PROTOCOL_MAGIC;
                hb.version = PROTOCOL_VERSION;
                hb.messageType = "HEARTBEAT";
                hb.sender = workerId;
                hb.timestamp = System.currentTimeMillis();
                hb.payload = new byte[0];

                sendMessage(hb);

            } catch (IOException e) {
                logger.log(Level.WARNING, "Heartbeat failed", e);
            }

        }, 2, 2, TimeUnit.SECONDS);
    }

    private void listenForMessages() {

        new Thread(() -> {

            try {
                while (!socket.isClosed()) {

                    int len = in.readInt();
                    byte[] frame = new byte[len];
                    in.readFully(frame);

                    Message msg = Message.unpack(frame);

                    if ("TASK".equals(msg.messageType)) {
                        workerPool.submit(() -> executeTask(msg));
                    }
                }

            } catch (IOException e) {
                logger.log(Level.WARNING, "Connection to Master lost", e);
            }

        }).start();
    }

    /**
     * Executes matrix block multiplication.
     */
    private void executeTask(Message msg) {

        long startTime = System.nanoTime();

        ByteBuffer buffer = ByteBuffer.wrap(msg.payload);
        buffer.order(ByteOrder.BIG_ENDIAN);

        int startRow = buffer.getInt();
        int endRow = buffer.getInt();

        int[][] A = deserializeMatrix(buffer);
        int[][] B = deserializeMatrix(buffer);

        int rows = endRow - startRow;
        int cols = B[0].length;
        int common = A[0].length;

        int[][] result = new int[rows][cols];

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                for (int k = 0; k < common; k++)
                    result[i][j] += A[i][k] * B[k][j];

        sendResult(startRow, result);

        long duration = (System.nanoTime() - startTime) / 1_000_000;
        logger.info("Task executed in " + duration + " ms.");
    }

    private void sendResult(int startRow, int[][] block) {

        try {
            int rows = block.length;
            int cols = block[0].length;

            ByteBuffer buffer = ByteBuffer.allocate(8 + rows * cols * 4);
            buffer.order(ByteOrder.BIG_ENDIAN);

            buffer.putInt(startRow);
            buffer.putInt(rows);

            for (int[] row : block)
                for (int val : row)
                    buffer.putInt(val);

            Message result = new Message();
            result.magic = PROTOCOL_MAGIC;
            result.version = PROTOCOL_VERSION;
            result.messageType = "RESULT";
            result.sender = workerId;
            result.timestamp = System.currentTimeMillis();
            result.payload = buffer.array();

            sendMessage(result);

        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed sending result", e);
        }
    }

    private int[][] deserializeMatrix(ByteBuffer buffer) {

        int rows = buffer.getInt();
        int cols = buffer.getInt();

        int[][] matrix = new int[rows][cols];

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                matrix[i][j] = buffer.getInt();

        return matrix;
    }

    private void sendMessage(Message msg) throws IOException {

        byte[] packed = msg.pack();

        synchronized (writeLock) {
            out.writeInt(packed.length);
            out.write(packed);
            out.flush();
        }
    }

    /**
     * Required by unit tests.
     * Delegates to internal worker lifecycle.
     */
    public void execute() {
        logger.info("Worker execute() invoked (test hook).");
    }

}
