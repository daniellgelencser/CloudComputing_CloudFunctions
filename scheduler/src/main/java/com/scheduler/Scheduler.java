package com.scheduler;

import java.util.logging.Logger;

import com.google.api.gax.paging.Page;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.storage.Blob;
// import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.scheduler.event.PubSubMessage;

public class Scheduler implements BackgroundFunction<PubSubMessage> {
    private static Storage storage = StorageOptions.getDefaultInstance().getService();
    private static final Logger logger = Logger.getLogger(Scheduler.class.getName());

    private String inputBucket;

    @Override
    public void accept(PubSubMessage message, Context context) throws Exception {

        if (message == null || message.getData() == null) {
            throw new Exception("Pub/Sub message empty");
        }

        
        inputBucket = System.getenv("INPUT_BUCKET");

        prepareJobs(message.getData());        
    }

    private void prepareJobs(String prefix) {
        // BucketInfo bucketInfo = BucketInfo.newBuilder(inputBucket).setVersioningEnabled(true).build();        
        Page<Blob> blobs = 
            storage.list(
                inputBucket,
                Storage.BlobListOption.prefix(prefix)
                );

        for(Blob blob: blobs.iterateAll())
        {
            logger.info(blob.getName());
        }

        // for (
            
    }
}
