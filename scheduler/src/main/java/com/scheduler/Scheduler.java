package com.scheduler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Base64;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.google.api.gax.paging.Page;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.scheduler.event.PubSubMessage;

public class Scheduler implements BackgroundFunction<PubSubMessage> {
    private static Storage storage = StorageOptions.getDefaultInstance().getService();
    private static final Logger logger = Logger.getLogger(Scheduler.class.getName());

    private static final String inputBucket = System.getenv("INPUT_BUCKET");
    private static final String dbConnection = System.getenv("DB_CONNECTION");
    private static final String dbUser = System.getenv("DB_USER");
    private static final String dbPass = System.getenv("DB_PASS");
    private static final String dbName = System.getenv("DB_NAME");
    private static final String projectId = System.getenv("GOOGLE_CLOUD_PROJECT");

    private DataSource connectionPool;

    @Override
    public void accept(PubSubMessage message, Context context) throws Exception {

        if (message == null || message.getData() == null) {
            logger.warning("Pub/Sub message empty");
            return;
        }

        String data = new String(Base64.getDecoder().decode(message.getData()));
        connectionPool = getMySqlConnectionPool();

        prepareJobs(data);

    }

    private void prepareJobs(String prefix) {
        Page<Blob> blobs = storage.list(
                inputBucket,
                Storage.BlobListOption.prefix(prefix));

        int count = 0;
        for (Blob blob : blobs.iterateAll()) {
            logger.info(blob.getName());
            insertSortJob(prefix, blob.getName());
            count++;
        }

        createMergeJobs(prefix, count);
        try {
            publishStartSorter(prefix);
        } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
            logger.severe(e.getMessage());
        }
    }

    public void publishStartSorter(String prefix) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        String topic = "start_sorter";
        logger.info("Publishing message to topic: " + topic);

        ByteString bStr = ByteString.copyFrom(prefix, StandardCharsets.UTF_8);
        PubsubMessage message = PubsubMessage.newBuilder().setData(bStr).build();

        Publisher pub = Publisher.newBuilder(ProjectTopicName.of(projectId, topic)).build();
        pub.publish(message).get();
    }

    public void createMergeJobs(String prefix, int chunkCount) {
        int x = 1;
        int round = 0;
        do {

            x *= 2;
            for (int i = 0; i < chunkCount; i += x) {
                int y = i + x / 2;
                if (y > chunkCount) {
                    continue;
                }
                insertMergeJob(prefix, prefix + "/chunk_" + i + ".txt", prefix + "/chunk_" + y + ".txt", round);
            }
            round++;

        } while (x < chunkCount);
    }

    public void insertMergeJob(String prefix, String chunk1, String chunk2, int round) {
        try {
            executeQuery(
                    "INSERT INTO `cloud_computing`.`job` (`prefix`, `type`, `chunk_one`, `chunk_two`, `status`)"
                            + "VALUES ('" + prefix + "', 'merge_r" + round + "', '" + chunk1 + "', '" + chunk2
                            + "', 'pending')");
        } catch (SQLException e) {
            logger.severe(e.getMessage());
        }
    }

    public void insertSortJob(String prefix, String chunk) {
        try {
            executeQuery(
                    "INSERT INTO `cloud_computing`.`job` (`prefix`, `type`, `chunk_one`, `status`)"
                            + "VALUES ('" + prefix + "', 'quicksort', '" + chunk + "', 'pending')");
        } catch (SQLException e) {
            logger.severe(e.getMessage());
        }
    }

    public void executeQuery(String query) throws SQLException {
        Connection connection = connectionPool.getConnection();
        PreparedStatement statement = connection.prepareStatement(query);
        statement.executeUpdate();
        connection.close();
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
}
