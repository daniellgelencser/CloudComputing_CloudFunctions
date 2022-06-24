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

import com.google.cloud.ReadChannel;
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
    private int chunkId;

    @Override
    public void accept(PubSubMessage message, Context context) {
        if (message == null || message.getData() == null) {
            logger.warning("Pub/Sub message empty");
            return;
        }

        String data = new String(Base64.getDecoder().decode(message.getData()));
        connectionPool = getMySqlConnectionPool();

        getChunkName(data);
        sortChunk();
    }

    private void getChunkName(String message) {
        String[] args = message.split(",");
        String prefix = args[0];
        chunkId = Integer.parseInt(args[1]);
        logger.info("Message is:"+message);
        logger.info("Processing chunk:"+chunkId+" , with prefix:"+prefix);
        try {
            ResultSet results = executeQuery(
                    "SELECT * FROM `job` "
                            + "WHERE `prefix` LIKE '" + prefix + "' "
                            + "AND `type` LIKE 'quicksort' "
                            + "AND `chunk_one` LIKE '" + prefix + "/chunk_" + chunkId + ".txt' "
                            + "LIMIT 1;");

            int jobId = results.getInt("id");
            if (results.wasNull()) {
                // sorting is done nothing to do
                return;
            }

            logger.info("Processing Job:"+jobId);

            int success = executeUpdate(
                    "UPDATE `job` "
                            + "SET `status` = 'in_progress' "
                            + "WHERE `id` = " + jobId+";");

            logger.info("Marked Job:"+jobId+" in_progress");
            if (success > 0) {
                chunkName = results.getString("chunk_one");
                nextChunk = prefix + "/chunk_" + chunkId + ".txt'";
                logger.info("Sorting file:"+chunkName);
            }

        } catch (SQLException e) {
            logger.severe(e.getMessage());
        }
    }

    private int executeUpdate(String query) throws SQLException {
        Connection connection = connectionPool.getConnection();
        PreparedStatement statement = connection.prepareStatement(query);
        int result = statement.executeUpdate();
        connection.close();
        return result;
    }

    private ResultSet executeQuery(String query) throws SQLException {
        Connection connection = connectionPool.getConnection();
        PreparedStatement statement = connection.prepareStatement(query);
        ResultSet results = statement.executeQuery();
        connection.close();
        return results;
    }

    private static DataSource getMySqlConnectionPool() {
        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(String.format("jdbc:mysql://%s", dbName));
        config.setUsername(dbUser);
        config.setPassword(dbPass);
        config.addDataSourceProperty("socketFactory", "com.google.cloud.sql.mysql.SocketFactory");
        config.addDataSourceProperty("cloudSqlInstance", dbConnection);
        config.addDataSourceProperty("ipTypes", "PUBLIC,PRIVATE");

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
            return str.substring(str.indexOf('\n', 0) + 1);
        } else {
            return str;
        }

    }

    private String getSecondPart() {
        Blob inputBlob = storage.get(BlobId.of(inputBucket, nextChunk));
        if (!inputBlob.exists()) {
            return "";
        }

        byte[] byteContent = storage.readAllBytes(inputBucket, nextChunk);
        String str = new String(byteContent);
        int endOffset = str.indexOf('\n', 0);
        if (endOffset != -1) {
            return str.substring(0, endOffset);
        } else {
            return str;
        }
    }

    private void sortChunk() {

        StringBuilder contentBuilder = new StringBuilder(getFirstPart());
        contentBuilder.append(getSecondPart());
        String[] lines = contentBuilder.toString().split("\n");
        Arrays.sort(lines);

        String content = String.join("\n", lines);
        try {
            writeChunk(content.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            logger.warning(e.getMessage());
        }
    }

    private void writeChunk(byte[] chunk) throws IOException {

        logger.info("Uploading chunk: " + chunkName);
        BlobId blobId = BlobId.of(outputBucket, chunkName);
        BlobInfo outputInfo = BlobInfo.newBuilder(blobId).build();

        WriteChannel writer = storage.writer(outputInfo);
        int writtenBytes = writer.write(ByteBuffer.wrap(chunk, 0, chunk.length));
        logger.info("Created file: " + chunkName + " with length of " + writtenBytes + "bytes");

        writer.close();
    }

    private void readChunk(ReadChannel reader, long size) throws IOException {

    }
}
