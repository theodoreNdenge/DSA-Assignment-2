import ballerina/io;
//import ballerina/log;
//import ballerina/time;
import ballerinax/kafka;

// Kafka Producer to send the response back to the central_logistics
kafka:Producer responseInternationalProducer = check new (KAFKA_BROKER_URL);

// Kafka Listener to listen for requests on the "international_delivery_requests" topic
listener kafka:Listener con = new (KAFKA_BROKER_URL, {
    groupId: "international-delivery-group",
    topics: "international_delivery_requests" // Kafka topic for international delivery requests
});

// Function to process the received Shipment and calculate the new delivery time
service on cons {

    remote function onConsumerRecord(Shipment[] requests) returns error? {
        foreach Shipment req in requests {
            io:println("Received International Delivery Request: ", req);

            // Add 7 days to the preferred delivery time
            string updatedDeliveryTime = check addSevenDaysToPreferredTime(req.preferredTime);

            // Create a mutable copy of the request and update the delivery time
            Shipment updatedReq = check req.cloneWithType();
            updatedReq.preferredTime = updatedDeliveryTime;

            // Log the updated delivery time
            io:println("Updated Delivery Time: ", updatedDeliveryTime);

            // Send the updated request back to the central_logistics Kafka topic
            check responseInternationalProducer->send({topic: "international_delivery_response", value: updatedReq});
            io:println("Response sent back to central logistics: ", updatedReq);
        }
    }
}

function addSevenDaysToPreferredTime(string preferredTime) returns string|error {
    // Manually split the preferredTime string (assumes the format "yyyy-MM-dd")
    string datePart = preferredTime.substring(0, 10); // Extracts "yyyy-MM-dd"

    // Split the date part into year, month, and day
    int year = check int:fromString(datePart.substring(0, 4)); // Extract year
    int month = check int:fromString(datePart.substring(5, 7)); // Extract month
    int day = check int:fromString(datePart.substring(8, 10)); // Extract day

    // Add 7 days
    day = day + 7;

    // Basic overflow handling (assuming 30 days per month)
    if (day > 30) {
        day = day - 30;
        month = month + 1;
    }

    // Handle month overflow (if month exceeds 12, increment the year)
    if (month > 12) {
        month = month - 12;
        year = year + 1;
    }

    // Convert month and day to strings and manually pad with '0' if needed
    string monthStr = month < 10 ? "0" + month.toString() : month.toString();
    string dayStr = day < 10 ? "0" + day.toString() : day.toString();

    // Format the updated date back to "yyyy-MM-dd"
    string updatedDate = year.toString() + "-" + monthStr + "-" + dayStr;

    // Combine the updated date with the original time part (assumes the time is "HH:mm a")
    string updatedDateTime = updatedDate + preferredTime.substring(10); // Adds the time back

    return updatedDateTime;
}
