package com.preprocessor;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
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
import java.util.logging.Logger;

public class Preprocessor implements BackgroundFunction<GcsEvent> {
  private static Storage storage = StorageOptions.getDefaultInstance().getService();
  private static final Logger logger = Logger.getLogger(Preprocessor.class.getName());

  private String inputBucket, outputBucket, fileName;

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
    outputBucket = System.getenv("OUTPUT_BUCKET").trim();
    fileName = event.getName();

    prepareChunks(100);
  }

  public byte[] readFileChunk(ReadChannel reader, long start, long chunkSize) throws IOException {
    long end = start + chunkSize;

    reader.seek(start);
    ByteBuffer bytes = ByteBuffer.allocate((int) (end - start));
    reader.read(bytes);
    bytes.flip();

    byte[] chunk = new byte[(int) chunkSize];
    int i = 0;
    while (bytes.hasRemaining()) {
      chunk[i++] = bytes.get();
    }

    return chunk;
  }

  public void writeChunk(byte[] chunk, int index) throws IOException {
    String chunkName = fileName.replace(".txt", "_chunk_" + index + ".txt");
    BlobInfo outputInfo = BlobInfo.newBuilder(outputBucket, chunkName).build();

    WriteChannel writer = storage.writer(outputInfo);
    writer.write(ByteBuffer.wrap(chunk, 0, chunk.length));
  }

  public void prepareChunks(long chunkSize) {

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

        logger.info(new String(chunk));
        start += chunkSize;

      } while (start < inputSize);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  // public void writeChunks()
  // {
  // int i = 0;
  // for (byte[] chunk : content) {

  // String chunkName = fileName.replace(".txt", "_chunk_" + i++ + ".txt");
  // BlobInfo info = BlobInfo.newBuilder(outputBucket, chunkName).build();

  // try (WriteChannel writer = storage.writer(info)) {

  // writer.write(ByteBuffer.wrap(chunk, 0, chunk.length));

  // } catch (IOException e) {
  // // TODO Auto-generated catch block
  // e.printStackTrace();
  // }
  // }
  // }

  // public void readFile(long blobSize)
  // {
  // Blob blob = storage.get(BlobId.of(inputBucket, fileName));
  // if(!blob.exists()) {
  // return;
  // }

  // long size = blob.getSize();
  // content = new byte[(int) Math.ceil(size / blobSize)][(int)blobSize];

  // long start = 0, end = 0;
  // try (ReadChannel reader = blob.reader()) {

  // do {

  // end = start + blobSize;

  // reader.seek(start);
  // ByteBuffer bytes = ByteBuffer.allocate((int) (end - start));
  // reader.read(bytes);
  // bytes.flip();

  // String text = "";
  // while (bytes.hasRemaining()) {
  // text += (char) bytes.get();
  // }

  // logger.info(text);

  // start = end;

  // } while (end < size);

  // reader.close();

  // } catch (IOException e) {
  // // TODO Auto-generated catch block
  // e.printStackTrace();
  // }
  // }

  // public void testFile(BlobInfo info)
  // {
  // String bucketName = info.getBucket();
  // String fileName = info.getName();

  // Blob blob = storage.get(BlobId.of(bucketName, fileName));
  // Path download = Paths.get("/tmp/", fileName);
  // blob.downloadTo(download);

  // File textFile = new File("/tmp/" + fileName);
  // ArrayList<String> list = new ArrayList<String>();

  // if(textFile.canRead()) {
  // try {
  // Scanner reader = new Scanner(textFile);

  // while (reader.hasNextLine()) {
  // String line = reader.nextLine();
  // // logger.info(line);
  // list.add(line);
  // }

  // reader.close();

  // } catch (FileNotFoundException e) {
  // // TODO Auto-generated catch block
  // e.printStackTrace();
  // }
  // }

  // Collections.sort(list);

  // for (String str : list) {
  // logger.info(str);
  // }

  // // Path upload = Paths.get("/tmp/upload/", more)

  // try {
  // Files.delete(download);
  // } catch (IOException e) {
  // // TODO Auto-generated catch block
  // e.printStackTrace();
  // }
  // // Files.delete(upload);
  // }
}
