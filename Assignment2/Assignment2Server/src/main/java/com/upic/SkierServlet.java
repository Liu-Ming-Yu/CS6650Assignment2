package com.upic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.annotation.WebServlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Pattern;

@WebServlet(name = "SkierServlet", urlPatterns = {"/skiers/*"})
public class SkierServlet extends HttpServlet {

    private static final String QUEUE_NAME = "LiftRideQueue";
    private static final String HOST = "3.230.249.177"; // Replace with your RabbitMQ host
    private static final int PORT = 5672; // Default RabbitMQ port
    private static final String USERNAME = "mliu"; // Replace with your RabbitMQ username
    private static final String PASSWORD = "hihbis-mUxgax-6derna"; // Replace with your RabbitMQ password

    private ConnectionFactory factory;

    @Override
    public void init() throws ServletException {
        super.init();
        // Initialize RabbitMQ connection factory
        factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(PORT);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);

    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // Set response content type to JSON
        response.setContentType("application/json");
        PrintWriter out = response.getWriter();

        // Validate and parse URL path
        String path = request.getPathInfo();
        String[] urlParts = path == null ? new String[]{} : path.split("/");

        if (!isUrlValid(urlParts)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            out.write("{\"message\": \"Invalid URL format.\"}");
            return;
        }

        // Extract path parameters
        String resortID = urlParts[1];
        String seasonID = urlParts[3];
        String dayID = urlParts[5];
        String skierID = urlParts[7];

        // Validate path parameters
        if (!isInteger(resortID) || !isInteger(skierID)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            out.write("{\"message\": \"Resort ID and Skier ID must be integers.\"}");
            return;
        }

        if (!isValidSeasonID(seasonID)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            out.write("{\"message\": \"Season ID must be a 4-digit year.\"}");
            return;
        }

        if (!isValidDayID(dayID)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            out.write("{\"message\": \"Day ID must be between 1 and 366.\"}");
            return;
        }

        // Parse and validate JSON payload
        ObjectMapper mapper = new ObjectMapper();
        JsonNode payload;
        try {
            payload = mapper.readTree(request.getReader());
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            out.write("{\"message\": \"Invalid JSON payload.\"}");
            return;
        }

        if (!isPayloadValid(payload)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            out.write("{\"message\": \"Invalid or missing fields in JSON payload.\"}");
            return;
        }

        // Format data to send to the queue
        LiftRide liftRide = new LiftRide();
        liftRide.setResortID(Integer.parseInt(resortID));
        liftRide.setSeasonID(seasonID);
        liftRide.setDayID(Integer.parseInt(dayID));
        liftRide.setSkierID(Integer.parseInt(skierID));
        liftRide.setTime(payload.get("time").asInt());
        liftRide.setLiftID(payload.get("liftID").asInt());

        // Send data to RabbitMQ queue
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            String message = mapper.writeValueAsString(liftRide);
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            // Log message to console
            System.out.println(" [x] Sent '" + message + "'");
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            out.write("{\"message\": \"Failed to send message to the queue.\"}");
            return;
        }

        // Return success response
        response.setStatus(HttpServletResponse.SC_CREATED);
        out.write("{\"message\": \"Lift ride recorded successfully.\"}");
    }

    // Helper method to validate the URL
    private boolean isUrlValid(String[] urlParts) {
        // URL pattern: /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
        if (urlParts.length != 8) {
            return false;
        }
        return urlParts[0].isEmpty() &&
                urlParts[1].matches("\\d+") &&
                "seasons".equals(urlParts[2]) &&
                urlParts[3].matches("\\d{4}") &&
                "days".equals(urlParts[4]) &&
                urlParts[5].matches("\\d{1,3}") &&
                "skiers".equals(urlParts[6]) &&
                urlParts[7].matches("\\d+");
    }

    // Helper method to validate if a string is an integer
    private boolean isInteger(String str) {
        return str != null && str.matches("\\d+");
    }

    // Helper method to validate seasonID
    private boolean isValidSeasonID(String seasonID) {
        return seasonID != null && seasonID.matches("\\d{4}");
    }

    // Helper method to validate dayID
    private boolean isValidDayID(String dayID) {
        if (!isInteger(dayID)) {
            return false;
        }
        int day = Integer.parseInt(dayID);
        return day >= 1 && day <= 366;
    }

    // Helper method to validate JSON payload
    private boolean isPayloadValid(JsonNode payload) {
        if (payload == null) {
            return false;
        }
        if (!payload.has("time") || !payload.has("liftID")) {
            return false;
        }
        if (!payload.get("time").isInt() || !payload.get("liftID").isInt()) {
            return false;
        }
        int time = payload.get("time").asInt();
        int liftID = payload.get("liftID").asInt();
        return time >= 0 && time <= 360 && liftID > 0;
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
