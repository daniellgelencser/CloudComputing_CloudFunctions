package com.sorter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Base64;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.google.cloud.WriteChannel;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.sorter.event.PubSubMessage;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Sorter implements BackgroundFunction<PubSubMessage> {
    private static Storage storage = StorageOptions.getDefaultInstance().getService();
    private static final Logger logger = Logger.getLogger(Sorter.class.getName());

    private static final String inputBucket = System.getenv("INPUT_BUCKET");
    private static final String dbConnection = System.getenv("DB_CONNECTION").strip();
    private static final String dbUser = System.getenv("DB_USER");
    private static final String dbPass = System.getenv("DB_PASS");
    private static final String dbName = System.getenv("DB_NAME");
    private static final String projectId = System.getenv("GOOGLE_CLOUD_PROJECT");
    public static final String outputBucket = System.getenv("OUTPUT_BUCKET");

    private DataSource connectionPool;
    private String chunkName, nextChunk;
    private String prefix;
    private int chunkId;
    private int jobId = -1;

    @Override
    public void accept(PubSubMessage message, Context context) throws SQLException {
        connectionPool = getMySqlConnectionPool();
        if (message == null || message.getData() == null) {
            logger.warning("Pub/Sub message empty");
            return;
        }

        String data = new String(Base64.getDecoder().decode(message.getData()));

        if (data != null || data.contains(",")) {
            prepareJob(data);
            markJobInProgress();
        }
        if (chunkName != null && nextChunk != null) {
            sortChunk();
            markChunkSorted();
        }
    }

    private void markChunkSorted() throws SQLException {
        int success = executeUpdate(
                "UPDATE `job` "
                        + "SET `status` = 'done' "
                        + "WHERE `id` = " + jobId + ";");
        if (success > 0)
            logger.info("Marked Job:" + jobId + " done!");
    }

    private void prepareJob(String message) {
        logger.info("Message is:" + message);
        String[] args = message.split(",");
        prefix = args[0];
        chunkId = Integer.parseInt(args[1]);
        logger.info("Processing chunk:" + chunkId + " , with prefix:" + prefix);
        try {
            String query = "SELECT id, chunk_one FROM `job` "
                    + "WHERE `prefix` LIKE '" + prefix + "' "
                    + "AND `type` LIKE 'quicksort' "
                    + "AND `chunk_one` LIKE '" + prefix + "/chunk_" + chunkId + ".txt' "
                    + "LIMIT 1;";
            logger.info(query);
            jobId = executeQuery(query);

            logger.info("Processing Job:" + jobId);
        } catch (SQLException e) {
            logger.severe(e.getMessage());
        }
    }

    private void markJobInProgress() throws SQLException {
        int success = executeUpdate(
                "UPDATE `job` "
                        + "SET `status` = 'in_progress' "
                        + "WHERE `id` = " + jobId + ";");

        logger.info("Marked Job:" + jobId + " in_progress");
        if (success > 0) {
            nextChunk = prefix + "/chunk_" + (chunkId + 1) + ".txt";
            logger.info("Sorting file:" + chunkName);
        }
    }

    private int executeUpdate(String query) throws SQLException {
        Connection connection = connectionPool.getConnection();
        PreparedStatement statement = connection.prepareStatement(query);
        int result = statement.executeUpdate();
        connection.close();
        return result;
    }

    private int executeQuery(String query) throws SQLException {
        Connection connection = null;
        try {
            connection = connectionPool.getConnection();
            PreparedStatement statement = connection.prepareStatement(query);
            ResultSet results = statement.executeQuery();

            while (results.next()) {
                jobId = results.getInt("id");
                chunkName = results.getString("chunk_one");
            }
            // if (results.wasNull()) {
            // // sorting is done nothing to do
            // return -1;
            // }
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.warning(e.toString());
            throw e;
        }

        return jobId;
    }

    private static DataSource getMySqlConnectionPool() {
        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(String.format("jdbc:mysql:///%s", dbName));
        config.setUsername(dbUser);
        config.setPassword(dbPass);
        config.addDataSourceProperty("socketFactory", "com.google.cloud.sql.mysql.SocketFactory");
        config.addDataSourceProperty("cloudSqlInstance", dbConnection);
        config.addDataSourceProperty("ipTypes", "PUBLIC,PRIVATE");
        config.addDataSourceProperty("databaseName", dbName);
        config.setMaximumPoolSize(5);

        return new HikariDataSource(config);
    }

    private String getFirstPart() {
        Blob inputBlob = storage.get(BlobId.of(inputBucket, chunkName));
        if (!inputBlob.exists()) {
            return "";
        }

        // discard incomplete first line
        byte[] byteContent = storage.readAllBytes(inputBucket, chunkName);
        String str = new String(byteContent);
        if (chunkId > 0) {
            int splitIndex = str.indexOf('\n') + 1;
            if (splitIndex > 0) {
                return str.substring(splitIndex);
            } else {
                return "";
            }
        } else {
            return str;
        }

    }

    private String getSecondPart() {
        logger.info("Accessing next chunk : " + nextChunk);
        Blob inputBlob = storage.get(BlobId.of(inputBucket, nextChunk));
        if (inputBlob == null || !inputBlob.exists()) {
            logger.info("Not Processing next Chunk:" + nextChunk);
            return "";
        }

        byte[] byteContent = storage.readAllBytes(inputBucket, nextChunk);
        String str = new String(byteContent);
        int endOffset = str.indexOf('\n');
        if (endOffset != -1) {
            return str.substring(0, endOffset);
        } else {
            return str;
        }
    }

    private void sortChunk() {
        logger.info("Sorting chunk file : " + chunkName);
        StringBuilder contentBuilder = new StringBuilder(getFirstPart());
        contentBuilder.append(getSecondPart());
        String[] lines = contentBuilder.toString().split("\n");
        Arrays.sort(lines);

        String content = String.join("\n", lines) + "\n";
        try {
            writeChunk(content.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            logger.warning(e.getMessage());
        }
    }

    private void writeChunk(byte[] chunk) throws IOException {
        String newFilename = prefix + "/r0_chunk_" + chunkId + ".txt";
        logger.info("Uploading chunk: " + newFilename);
        BlobId blobId = BlobId.of(outputBucket, newFilename);
        BlobInfo outputInfo = BlobInfo.newBuilder(blobId).build();

        WriteChannel writer = storage.writer(outputInfo);
        int writtenBytes = writer.write(ByteBuffer.wrap(chunk, 0, chunk.length));
        logger.info("Created file: " + newFilename + " with length of " + writtenBytes + "bytes");

        writer.close();
    }
}
