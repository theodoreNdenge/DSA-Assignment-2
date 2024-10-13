import ballerina/io;
import ballerina/log;
import ballerinax/kafka;
import ballerinax/mongodb;

configurable string host = "localhost";
configurable int port = 27017;
configurable string database = "logistics";

// MongoDB client configuration
mongodb:Client mongoDb = check new ({
    connection: {
        serverAddress: {
            host: "localhost",
            port: 27017
        }
    }
});

// Define a Request type to hold information about a delivery request
type Shipment record {
    string shipmentType; // Type of shipment (standard, express, international)
    string pickupLocation; // Pickup location for the package
    string deliveryLocation; // Delivery location for the package
    string preferredTime; // Preferred pickup or delivery time
    string customerFirstName; // Customer's first name
    string customerLastName; // Customer's last name
    string customerContact; // Customer's contact number
};

const string KAFKA_BROKER_URL = "localhost:9092";

// Kafka Producer clients for each delivery service
kafka:Producer standardProducer = check new (KAFKA_BROKER_URL);
kafka:Producer expressProducer = check new (KAFKA_BROKER_URL);
kafka:Producer internationalProducer = check new (KAFKA_BROKER_URL);

// Kafka Listener to receive messages
listener kafka:Listener cons = new (KAFKA_BROKER_URL, {
    groupId: "logistics-group",
    topics: "logistics_requests" // Kafka topic to listen to customer requests
});

// Function to handle received package requests
service on cons {

    private final mongodb:Database logisticsDb;

    function init() returns error? {
        self.logisticsDb = check mongoDb->getDatabase("logistics");
        log:printInfo("Kafka listener initialized for topic 'logistics_requests'.");
    }

    // Function to consume and process incoming records
    remote function onConsumerRecord(Shipment[] requests) returns error? {
        foreach Shipment req in requests {
            io:println("Received request: ", req);

            // Store the request in MongoDB
            mongodb:Collection shipmentsCollection = check self.logisticsDb->getCollection("shipments");

            // MongoDB collection for storing requests
            check shipmentsCollection->insertOne(req);
            log:printInfo("Request stored successfully in MongoDB.");

            // Call the appropriate delivery service based on the shipment type
            match req.shipmentType {
                "standard" => {
                    io:println("Routing to Standard Delivery Service...");
                    // Send the message to the "standard_delivery_requests" Kafka topic
                    check standardProducer->send({topic: "standard_delivery_requests", value: req});
                }
                "express" => {
                    io:println("Routing to Express Delivery Service...");
                    // Send the message to the "express_delivery_requests" Kafka topic
                    check expressProducer->send({topic: "express_delivery_requests", value: req});
                }
                "international" => {
                    io:println("Routing to International Delivery Service...");
                    // Send the message to the "international_delivery_requests" Kafka topic
                    check internationalProducer->send({topic: "international_delivery_requests", value: req});
                }
                _ => {
                    io:println("Unknown shipment type: ", req.shipmentType);
                }
            }
        }
    }
}

