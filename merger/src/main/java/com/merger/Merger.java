package com.merger;

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

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.merger.Merger.GCSEvent;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

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

    private DataSource connectionPool;
    private int round, leftId, rightId, jobId;
    private String prefix, leftFileName, rightFilename;
    private boolean ready = false;

    @Override
    public void accept(GCSEvent event, Context context) throws SQLException {
        logger.info("Processing file: " + event.name);

        prepareFields(event.name); // Get necessary fields and check if pair file is ready
        if(!ready){
            logger.info("When pair is ready, processing will start");
            return;
        }

        connectionPool = getMySqlConnectionPool();
        prepareJob();
        markJobInProgress();
    }

    private void prepareJob() {
        try {
            String query = "SELECT id, chunk_two FROM `job` "
                    + "WHERE `prefix` LIKE '" + prefix + "' "
                    + "AND `type` LIKE 'merge_r"+round+"' "
                    + "AND `chunk_one` LIKE '" + leftFileName +"'"
                    // + "AND `chunk_two` LIKE '" + rightFilename +"'"
                    + "LIMIT 1;";
            logger.info("Query:"+query);
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
            logger.info("Merging files -> left:" + leftFileName+" , right:"+rightFilename);
        }
    }

    private void prepareFields(String sortedFilename) {
        String[] firstSplit = sortedFilename.split("/");
        prefix = firstSplit[0];
        String filename = firstSplit[1];
        String[] fields = filename.split("_");
        round = Integer.parseInt(fields[0].substring(1));
        int chunkId = Integer.parseInt(fields[2].substring(0, fields[2].indexOf(".")));
        if (chunkId % 2 == 0) {
            leftId = chunkId;
            rightId = leftId + 1;
            leftFileName = sortedFilename;
            rightFilename = prefix + "/r" + round + "_chunk_" + rightId + ".txt";
            ready = isMergeReady(rightFilename);
        } else {
            leftId = chunkId - 1;
            rightId = chunkId;
            leftFileName = prefix + "/r" + round + "_chunk_" + leftId + ".txt";
            rightFilename = sortedFilename;
            ready = isMergeReady(leftFileName);
        }
    }

    private boolean isMergeReady(String filename) {
        Blob inputBlob = storage.get(BlobId.of(inputBucket, filename));
        if (inputBlob == null || !inputBlob.exists()) {
            logger.info("Merge pair not ready:" + filename);
            return false;
        } else {
            return true;
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
                rightFilename = results.getString("chunk_two");
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

    public static class GCSEvent {
        String bucket;
        String name;
        String metageneration;
    }
}
