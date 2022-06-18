package com.sorter;

import com.scheduler.event.PubSubMessage;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import java.util.Base64;
import java.util.Map;
import java.util.logging.Logger;

public class Sorter implements BackgroundFunction<PubSubMessage> {
  private static final Logger logger = Logger.getLogger(Sorter.class.getName());

  @Override
  public void accept(PubSubMessage message, Context context) {
    String data = message.getData() != null
      ? new String(Base64.getDecoder().decode(message.getData()))
      : "Hello, World";
    logger.info(data);
  }
}
