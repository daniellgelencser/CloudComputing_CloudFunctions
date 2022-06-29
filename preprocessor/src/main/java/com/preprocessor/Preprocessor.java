package com.preprocessor;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.preprocessor.event.GcsEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public class Preprocessor implements BackgroundFunction<GcsEvent> {
  private static Storage storage = StorageOptions.getDefaultInstance().getService();
  private static final Logger logger = Logger.getLogger(Preprocessor.class.getName());

  public static final String outputBucket = System.getenv("OUTPUT_BUCKET");
  public static final String projectId = System.getenv("GOOGLE_CLOUD_PROJECT");

  private static final int chunkSize = 4 * 1024 * 1024; // X MB

  private String inputBucket, fileName;

  @Override
  public void accept(GcsEvent event, Context context) {
    logger.info("Event: " + context.eventId());
    logger.info("Event Type: " + context.eventType());
    logger.info("Bucket: " + event.getBucket());
    logger.info("File: " + event.getName());
    logger.info("Metageneration: " + event.getMetageneration());
    logger.info("Created: " + event.getTimeCreated());
    logger.info("Updated: " + event.getUpdated());

    inputBucket = event.getBucket();
    fileName = event.getName();

    prepareChunks();
    try {
      publishStartScheduler();
    } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
      logger.severe(e.getMessage());
    }
  }

  public void publishStartScheduler() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    String topic = "start_scheduler";
    logger.info("Publishing message to topic: " + topic);

    String prefix = fileName.replace(".txt", "");
    ByteString bStr = ByteString.copyFrom(prefix, StandardCharsets.UTF_8);
    PubsubMessage message = PubsubMessage.newBuilder().setData(bStr).build();

    Publisher pub = Publisher.newBuilder(ProjectTopicName.of(projectId, topic)).build();
    pub.publish(message).get(10, TimeUnit.SECONDS);
  }

  private byte[] readFileChunk(ReadChannel reader, long start, long chunkSize) throws IOException {

    reader.seek(start);
    ByteBuffer bytes = ByteBuffer.allocate((int) chunkSize);
    reader.read(bytes);
    bytes.flip();

    byte[] chunk = new byte[(int) chunkSize];
    int i = 0;
    while (bytes.hasRemaining()) {
      chunk[i++] = bytes.get();
    }
    if (i < chunkSize) {
      return Arrays.copyOfRange(chunk, 0, i);
    } else {
      return chunk;
    }
  }

  private void writeChunk(byte[] chunk, int index) throws IOException {
    String chunkName = fileName.replace(".txt", "");
    chunkName = chunkName + "/chunk_" + index + ".txt";

    logger.info("Uploading chunk: " + chunkName);
    BlobId blobId = BlobId.of(outputBucket, chunkName);
    BlobInfo outputInfo = BlobInfo.newBuilder(blobId).build();

    WriteChannel writer = storage.writer(outputInfo);
    int writtenBytes = writer.write(ByteBuffer.wrap(chunk, 0, chunk.length));
    logger.info("Created file: " + chunkName + " with length of " + writtenBytes + "bytes");

    writer.close();
  }

  private void prepareChunks() {

    Blob inputBlob = storage.get(BlobId.of(inputBucket, fileName));
    if (!inputBlob.exists()) {
      return;
    }

    long inputSize = inputBlob.getSize();

    long start = 0;
    int index = 0;
    try (ReadChannel reader = inputBlob.reader()) {

      do {

        byte[] chunk = readFileChunk(reader, start, chunkSize);
        writeChunk(chunk, index++);

        // logger.info(new String(chunk));
        start += chunkSize;

      } while (start < inputSize);

      reader.close();
    } catch (IOException e) {
      logger.severe(e.getMessage());
    }
  }

}
