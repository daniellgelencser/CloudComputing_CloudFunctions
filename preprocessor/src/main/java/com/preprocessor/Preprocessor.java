package com.preprocessor;

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.preprocessor.event.GcsEvent;
import java.util.logging.Logger;

public class Preprocessor implements BackgroundFunction<GcsEvent> {
  private static final Logger logger = Logger.getLogger(Preprocessor.class.getName());

  @Override
  public void accept(GcsEvent event, Context context) {
    logger.info("Event: " + context.eventId());
    logger.info("Event Type: " + context.eventType());
    logger.info("Bucket: " + event.getBucket());
    logger.info("File: " + event.getName());
    logger.info("Metageneration: " + event.getMetageneration());
    logger.info("Created: " + event.getTimeCreated());
    logger.info("Updated: " + event.getUpdated());
  }
}
