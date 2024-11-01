package org.mliu;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LiftRideConsumer {

    private static final String QUEUE_NAME = "LiftRideQueue";
    private static final String HOST = "3.230.249.177"; // Replace with your RabbitMQ host
    private static final int PORT = 5672; // Default RabbitMQ port
    private static final String USERNAME = "mliu"; // Replace with your RabbitMQ username
    private static final String PASSWORD = "hihbis-mUxgax-6derna"; // Replace with your RabbitMQ password

    private static final int THREAD_POOL_SIZE = 10; // Adjust based on your needs

    // Thread-safe HashMap to store lift rides per skier
    private static ConcurrentHashMap<Integer, CopyOnWriteArrayList<LiftRide>> skierLiftRides = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {

        // Initialize RabbitMQ connection factory
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(PORT);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);

        // Create a fixed thread pool
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        // Create a connection and channel
        Connection connection = factory.newConnection();

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // Create a consumer for each thread
        for (int i = 0; i < THREAD_POOL_SIZE; i++) {
            executor.submit(() -> {
                try {
                    Channel threadChannel = connection.createChannel();
                    threadChannel.queueDeclare(QUEUE_NAME, true, false, false, null);
                    threadChannel.basicQos(1); // Fair dispatch

                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        String message = new String(delivery.getBody(), "UTF-8");
                        try {
                            processMessage(message);
                            // Manually acknowledge message after processing
                            threadChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        } catch (Exception e) {
                            e.printStackTrace();
                            // Optionally, reject the message and requeue or send to a dead-letter queue
                            threadChannel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
                        }
                    };

                    // Start consuming messages
                    threadChannel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});

                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        // Add shutdown hook to close resources
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);
                connection.close();
                System.out.println("Consumer shutdown gracefully.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }

    // Method to process each message
    private static void processMessage(String message) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            LiftRide liftRide = mapper.readValue(message, LiftRide.class);

            // Get the list of lift rides for the skier
            skierLiftRides.computeIfAbsent(liftRide.getSkierID(), k -> new CopyOnWriteArrayList<>()).add(liftRide);

            // For demonstration, print the processed message
            System.out.println("Processed lift ride for skier ID: " + liftRide.getSkierID());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

// LiftRide class to represent the data
class LiftRide {
    private int resortID;
    private String seasonID;
    private int dayID;
    private int skierID;
    private int time;
    private int liftID;

    // Getters and Setters
    public int getResortID() {
        return resortID;
    }

    public void setResortID(int resortID) {
        this.resortID = resortID;
    }

    public String getSeasonID() {
        return seasonID;
    }

    public void setSeasonID(String seasonID) {
        this.seasonID = seasonID;
    }

    public int getDayID() {
        return dayID;
    }

    public void setDayID(int dayID) {
        this.dayID = dayID;
    }

    public int getSkierID() {
        return skierID;
    }

    public void setSkierID(int skierID) {
        this.skierID = skierID;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public int getLiftID() {
        return liftID;
    }

    public void setLiftID(int liftID) {
        this.liftID = liftID;
    }
}
