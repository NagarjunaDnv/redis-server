package org.example.redisserver;

import org.example.redisserver.data.DataProcessor;
import org.example.redisserver.data.DataStore;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;


public class RedisServer {
    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
//    private final ExecutorService singleThreadedExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, taskQueue);
    private final ExecutorService singleThreadedExecutor = Executors.newSingleThreadExecutor();
    private static final String PERSISTED_DATA_FILE = System.getenv().getOrDefault("dataDir", "/tmp/data.txt");

    public void start(int port) {

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            DataStore dataStore = new DataStore();
            DataProcessor dataProcessor = new DataProcessor(dataStore);
            dataProcessor.load(PERSISTED_DATA_FILE);

            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                    () -> dataProcessor.persist(PERSISTED_DATA_FILE),
                    0,
                    30,
                    TimeUnit.SECONDS
            );

            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                    dataProcessor::removeExpiredKeys,
                    0,
                    15,
                    TimeUnit.SECONDS
            );

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Saving the final snapshot before exiting.");
                dataProcessor.persist(PERSISTED_DATA_FILE);
                System.out.println("DB saved on disk");
                System.out.println("Redis server is ready to exit, bye bye...");
            }));

            while(true) {
                Socket clientSocket = serverSocket.accept();
                singleThreadedExecutor.execute(new ClientHandler(clientSocket, dataProcessor));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args) {
        RedisServer redisServer = new RedisServer();
        redisServer.start(6379);
    }
}
