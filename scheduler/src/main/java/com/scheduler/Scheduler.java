package com.scheduler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.google.api.gax.paging.Page;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.storage.Blob;
// import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.scheduler.event.PubSubMessage;

public class Scheduler implements BackgroundFunction<PubSubMessage> {
    private static Storage storage = StorageOptions.getDefaultInstance().getService();
    private static final Logger logger = Logger.getLogger(Scheduler.class.getName());

    private static final String inputBucket = System.getenv("INPUT_BUCKET");
    private static final String dbConnection = System.getenv("DB_CONNECTION");
    private static final String dbUser = System.getenv("DB_USER");
    private static final String dbPass = System.getenv("DB_PASS");
    private static final String dbName = System.getenv("DB_NAME");

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
        // try {
        //     executeQuery(
        //             "INSERT INTO `cloud_computing`.`job` (`file_name`, `type`, `chunk_one`, `status`)"
        //                     + "VALUES ('testfile', 'testtype', 'testchunk', 'teststatus')");
        // } catch (SQLException e) {
        //     logger.severe(e.getMessage());
        // }

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
    }

    public void pubsubStartSort()
    {

    }

    public void createMergeJobs(String prefix, int chunkCount)
    {
        int x = 1;
        do {

            x *= 2;
            for(int i = 0; i < chunkCount; i += x) {
                int y = i + x/2;
                if (y > chunkCount) {
                    continue;
                }
                insertMergeJob(prefix, prefix + "/chunk_" + i + ".txt", prefix + "/chunk_" + y + ".txt");
            }

        }
        while (x < chunkCount);
    }

    public void insertMergeJob(String prefix, String chunk1, String chunk2)
    {
        try {
            executeQuery(
                "INSERT INTO `cloud_computing`.`job` (`prefix`, `type`, `chunk_one`, `chunk_two`, `status`)"
                + "VALUES ('" + prefix + "', 'quicksort', '" + chunk1 + "', '" + chunk2 + "', 'pending')");
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

    public boolean executeQuery(String query) throws SQLException {
        Connection connection = connectionPool.getConnection();
        PreparedStatement statement = connection.prepareStatement(query);
        return statement.execute();
    }

    private static DataSource getMySqlConnectionPool() {
        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(String.format("jdbc:mysql://%s", dbName));
        config.setUsername(dbUser);
        config.setPassword(dbPass);
        config.addDataSourceProperty("socketFactory", "com.google.cloud.sql.mysql.SocketFactory");
        config.addDataSourceProperty("cloudSqlInstance", dbConnection);
        config.addDataSourceProperty("ipTypes", "PUBLIC,PRIVATE");
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(10000); // 10s
        config.setIdleTimeout(600000); // 10m
        config.setMaxLifetime(1800000); // 30m

        return new HikariDataSource(config);
    }
}
