package com.scheduler;

import java.util.Base64;
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
            logger.warning("Pub/Sub message empty");
            return;
        }

        String data = new String(Base64.getDecoder().decode(message.getData()));
        
        inputBucket = System.getenv("INPUT_BUCKET");

        logger.info(data);

        prepareJobs(data);        
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
