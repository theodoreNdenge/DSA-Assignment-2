import ballerina/io;
//import ballerina/log;
import ballerinax/kafka;

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

// Main function to simulate a producer sending a request
public function main() returns error? {
    kafka:Producer prod = check new (KAFKA_BROKER_URL);

    // Creating a dummy shipment request to simulate sending a message
    Shipment msg = {
        shipmentType: "express",  // Change this to simulate different shipment types
        pickupLocation: "123 Pickup St.",
        deliveryLocation: "456 Delivery Ave.",
        preferredTime: "2024-10-15 10:00 AM",
        customerFirstName: "John",
        customerLastName: "Doe",
        customerContact: "123-456-7890"
    };

    // Send the request to the Kafka topic "logistics_request"
    check prod->send({topic: "logistics_requests", value: msg});
    io:println("Request sent to Kafka topic 'logistics_requests'.");
}

// Kafka Producer clients for each delivery service
kafka:Producer standardProducer = check new (KAFKA_BROKER_URL);
kafka:Producer expressProducer = check new (KAFKA_BROKER_URL);
kafka:Producer internationalProducer = check new (KAFKA_BROKER_URL);
