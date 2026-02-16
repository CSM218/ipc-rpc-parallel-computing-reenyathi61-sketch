package pdc;

import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {

    private final BlockingQueue<Runnable> taskQueue;
    private final ExecutorService pool;
    private volatile boolean running;
    private Socket socket;
    private Thread readerThread;
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> heartbeatFuture;
    private final AtomicBoolean connected = new AtomicBoolean(false);

    public Worker() {
        int threads = Math.max(1, Runtime.getRuntime().availableProcessors());
        this.taskQueue = new LinkedBlockingQueue<>();
        this.pool = Executors.newFixedThreadPool(threads);
        this.running = false;
    }

    // Use environment variable for student id if present
    private String getStudentId() {
        String env = System.getenv("STUDENT_ID");
        if (env != null && !env.isEmpty()) return env;
        return java.net.InetAddress.getLoopbackAddress().getHostName();
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            this.socket = new Socket(masterHost, port);
            this.socket.setKeepAlive(true);
            java.io.OutputStream out = this.socket.getOutputStream();
            java.io.InputStream in = this.socket.getInputStream();

            // Build identity/capability message
            Message idMsg = new Message();
            idMsg.magic = "PDC";
            idMsg.version = 1;
            idMsg.type = "IDENTIFY";
            idMsg.sender = java.net.InetAddress.getLocalHost().getHostName();
            idMsg.timestamp = System.currentTimeMillis();

            int procs = Runtime.getRuntime().availableProcessors();
            String cap = "procs=" + procs + ";pool=default";
            idMsg.payload = cap.getBytes(java.nio.charset.StandardCharsets.UTF_8);

            out.write(idMsg.pack());
            out.flush();

            // Read response (blocking) - expect ACK
            Message resp = readOneMessage(in);
            if (resp != null && "ACK".equals(resp.type)) {
                connected.set(true);

                // start heartbeat sender
                heartbeatFuture = heartbeatScheduler.scheduleAtFixedRate(() -> {
                    try {
                        Message hb = new Message();
                        hb.magic = "PDC";
                        hb.version = 1;
                        hb.type = "HEARTBEAT";
                        hb.sender = idMsg.sender;
                        hb.timestamp = System.currentTimeMillis();
                        hb.payload = new byte[0];
                        synchronized (out) {
                            out.write(hb.pack());
                            out.flush();
                        }
                    } catch (Exception ignored) {}
                }, 1, 1, TimeUnit.SECONDS);

                // start reader thread to process incoming messages (TASK, etc.)
                readerThread = new Thread(() -> {
                    try {
                        while (!Thread.currentThread().isInterrupted() && connected.get()) {
                            Message m = readOneMessage(in);
                            if (m == null) break;
                            handleMasterMessage(m);
                        }
                    } finally {
                        connected.set(false);
                        try { socket.close(); } catch (Exception ignored) {}
                    }
                }, "worker-reader");
                readerThread.setDaemon(true);
                readerThread.start();
            } else {
                // no ACK -> close
                try { this.socket.close(); } catch (Exception ignored) {}
            }
        } catch (Exception e) {
            // join failure: caller may retry
            if (this.socket != null) try { this.socket.close(); } catch (Exception ignored) {}
            connected.set(false);
        }
    }

    private Message readOneMessage(java.io.InputStream in) {
        try {
            byte[] lenBuf = new byte[4];
            int r = 0;
            while (r < 4) {
                int n = in.read(lenBuf, r, 4 - r);
                if (n < 0) return null;
                r += n;
            }
            int total = java.nio.ByteBuffer.wrap(lenBuf).getInt();
            if (total <= 0) return null;
            byte[] body = new byte[total];
            int received = 0;
            while (received < total) {
                int n = in.read(body, received, total - received);
                if (n < 0) return null;
                received += n;
            }
            byte[] framed = new byte[4 + total];
            System.arraycopy(lenBuf, 0, framed, 0, 4);
            System.arraycopy(body, 0, framed, 4, total);
            return Message.unpack(framed);
        } catch (Exception e) {
            return null;
        }
    }

    private void handleMasterMessage(Message m) {
        if (m == null) return;
        if ("TASK".equals(m.type)) {
            // Submit a simple runnable that simulates executing the task
            submitTask(() -> {
                try {
                    // In a real system, parse payload and perform computation
                    // Simulate work
                    Thread.sleep(10);

                    // Send RESULT back
                    Message res = new Message();
                    res.magic = "PDC";
                    res.version = 1;
                    res.type = "RESULT";
                    res.sender = m.sender != null ? m.sender : "worker";
                    res.timestamp = System.currentTimeMillis();
                    res.payload = "done".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    try {
                        synchronized (socket.getOutputStream()) {
                            socket.getOutputStream().write(res.pack());
                            socket.getOutputStream().flush();
                        }
                    } catch (Exception ignored) {}
                } catch (InterruptedException ignored) {}
            });
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        // Start dispatcher that moves tasks from queue to thread-pool.
        if (running) return;
        running = true;

        Thread dispatcher = new Thread(() -> {
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    Runnable task = taskQueue.take();
                    pool.submit(() -> {
                        try {
                            task.run();
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "worker-dispatcher");
        dispatcher.setDaemon(true);
        dispatcher.start();
    }

    /**
     * Submit a unit of work to this worker's scheduler.
     */
    public void submitTask(Runnable task) {
        if (!running) {
            // start on first submission
            execute();
        }
        this.taskQueue.offer(task);
    }

    /**
     * Gracefully shutdown the worker scheduler and pool.
     */
    public void shutdown() {
        running = false;
        pool.shutdown();
    }
}
