package com.example.eventHub;


import com.azure.messaging.eventhubs.*;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import com.azure.core.util.BinaryData;

public class Producer {

    private static final String connectionString = "";
    private static final String eventHubName = "iwbderserv";

    public static void main(String[] args) {
        EventHubProducerClient producer = new EventHubClientBuilder()
                .connectionString(connectionString, eventHubName)
                .buildProducerClient();

        // Create a batch
        EventDataBatch batch = producer.createBatch();

        for (int i = 0; i < 5; i++) {
            String message = "Hey I am Angad and I'm counting up to .... " + i;
            EventData eventData = new EventData(BinaryData.fromString(message));

            if (!batch.tryAdd(eventData)) {
                System.out.println("Event too large for the batch. Skipping: " + message);
            }
        }

        // Send the batch
        producer.send(batch);
        System.out.println("A batch of 5 events has been published.");

        // Close the producer
        producer.close();
    }
}
