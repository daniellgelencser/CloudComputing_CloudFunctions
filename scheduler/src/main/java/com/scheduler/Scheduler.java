package com.scheduler;

import java.util.logging.Logger;

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.scheduler.event.PubSubMessage;

public class Scheduler implements BackgroundFunction<PubSubMessage> {
    private static Storage storage = StorageOptions.getDefaultInstance().getService();
    private static final Logger logger = Logger.getLogger(Scheduler.class.getName());

    private String inputBucket;

    @Override
    public void accept(PubSubMessage payload, Context context) {
        
        inputBucket = System.getenv("INPUT_BUCKET");

        prepareJobs();        
    }

    private void prepareJobs() {
        BucketInfo bucketInfo = BucketInfo.newBuilder(inputBucket).setVersioningEnabled(true).build();        storage.get(blob)
    }
}
