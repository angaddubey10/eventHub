package com.example.eventHub;


import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

import java.util.function.Consumer;

public class EventHubConsumerApp {

    private static final String EH_NAMESPACE_CONNECTION_STRING = Config.EH_NAMESPACE_CONNECTION_STRING;
    private static final String EVENT_HUB_NAME = Config.EVENT_HUB_NAME;
    private static final String BLOB_STORAGE_CONNECTION_STRING = Config.BLOB_STORAGE_CONNECTION_STRING;
    private static final String BLOB_CONTAINER_NAME = Config.BLOB_CONTAINER_NAME;

    public static void ConsumerMain() {
        // Create async blob container client
        BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
                .connectionString(BLOB_STORAGE_CONNECTION_STRING)
                .containerName(BLOB_CONTAINER_NAME)
                .buildAsyncClient();

        // Create the processor client
        EventProcessorClient processorClient = new EventProcessorClientBuilder()
                .connectionString(EH_NAMESPACE_CONNECTION_STRING, EVENT_HUB_NAME)
                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .processEventBatch(eventBatchProcessor, 1)
                .processError(errorHandler)
                .checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient))
                .buildEventProcessorClient();

        // Start processing
        processorClient.start();
        System.out.println("Listening to Event Hub... Press ENTER to stop.");
        try {
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Stop processing
        processorClient.stop();
        System.out.println("Stopped.");
    }

    private static final Consumer<EventContext> eventBatchProcessor = context -> {
        for (EventData eventData : context.getEvents()) {
            String message = eventData.getBodyAsString();
            System.out.printf("Event received from partition %s: %s%n",
                    context.getPartitionContext().getPartitionId(), message);
        }
        context.updateCheckpoint();
    };

    private static final Consumer<ErrorContext> errorHandler = context -> {
        System.out.printf("Error on partition %s: %s%n",
                context.getPartitionContext().getPartitionId(),
                context.getThrowable().getMessage());
    };
}
