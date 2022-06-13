package com.preprocessor;

import java.io.File;
import java.io.FileNotFoundException;

import com.google.cloud.ReadChannel;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.preprocessor.event.GcsEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;
import java.util.logging.Logger;

public class Preprocessor implements BackgroundFunction<GcsEvent> {
  private static Storage storage = StorageOptions.getDefaultInstance().getService();
  private static final Logger logger = Logger.getLogger(Preprocessor.class.getName());

  private static String chunksBucket = "cloud-computing-352913-bucket-chunks";

  @Override
  public void accept(GcsEvent event, Context context) {
    logger.info("Event: " + context.eventId());
    logger.info("Event Type: " + context.eventType());
    logger.info("Bucket: " + event.getBucket());
    logger.info("File: " + event.getName());
    logger.info("Metageneration: " + event.getMetageneration());
    logger.info("Created: " + event.getTimeCreated());
    logger.info("Updated: " + event.getUpdated());

    BlobInfo info = BlobInfo.newBuilder(event.getBucket(), event.getName()).build();

    readFile(info, 10);
  }

  public void readFile(BlobInfo info, long blobSize)
  {
    String inputBucket = info.getBucket();
    String fileName = info.getName();

    Blob blob = storage.get(BlobId.of(inputBucket, fileName));
    long size = blob.getSize();

    long start = 1, end = 0;
    try (ReadChannel reader = blob.reader()) {

      do {

        end = start + blobSize -1;
        String text = getText(reader, start, end);
        logger.info(text);

        start = end + 1;

      } while (end < size);
      
      reader.close();

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private String getText(ReadChannel reader, long start, long end) throws IOException {
    reader.seek(start);
    ByteBuffer bytes = ByteBuffer.allocate((int) (end - start));
    reader.read(bytes);
    bytes.flip();

    String text = "";
    while (bytes.hasRemaining()) {
      text += (char) bytes.get();
    }
    return text;
  }

  public void testFile(BlobInfo info)
  {
    String bucketName = info.getBucket();
    String fileName = info.getName();

    Blob blob = storage.get(BlobId.of(bucketName, fileName));
    Path download  = Paths.get("/tmp/", fileName);
    blob.downloadTo(download);

    File textFile = new File("/tmp/" + fileName);
    ArrayList<String> list = new ArrayList<String>();

    if(textFile.canRead()) {
      try {
        Scanner reader = new Scanner(textFile);

        while (reader.hasNextLine()) {
          String line = reader.nextLine();
          // logger.info(line);
          list.add(line);
        }

        reader.close();

      } catch (FileNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    Collections.sort(list);

    for (String str : list) {
      logger.info(str);
    }
    
    // Path upload = Paths.get("/tmp/upload/", more)

    try {
      Files.delete(download);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    // Files.delete(upload);
  }
}
