package com.merger;

import java.util.logging.Logger;

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.merger.Merger.GCSEvent;

public class Merger implements BackgroundFunction<GCSEvent> {
  private static Storage storage = StorageOptions.getDefaultInstance().getService();
  private static final Logger logger = Logger.getLogger(Merger.class.getName());
  private static final String inputBucket = System.getenv("INPUT_BUCKET");
  private static final String dbConnection = System.getenv("DB_CONNECTION").strip();
  private static final String dbUser = System.getenv("DB_USER");
  private static final String dbPass = System.getenv("DB_PASS");
  private static final String dbName = System.getenv("DB_NAME");
  private static final String projectId = System.getenv("GOOGLE_CLOUD_PROJECT");
  public static final String outputBucket = System.getenv("OUTPUT_BUCKET");

  @Override
  public void accept(GCSEvent event, Context context) {
    logger.info("Processing file: " + event.name);
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public static class GCSEvent {
    String bucket;
    String name;
    String metageneration;
  }
}
