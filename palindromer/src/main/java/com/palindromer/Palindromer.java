package com.palindromer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.google.cloud.ReadChannel;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.palindromer.Palindromer.GCSEvent;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Palindromer implements BackgroundFunction<GCSEvent> {
    // private static Storage storage =
    // StorageOptions.getDefaultInstance().getService();
    private static final Logger logger = Logger.getLogger(Palindromer.class.getName());
    private static final String inputBucket = System.getenv("INPUT_BUCKET");
    private static final String dbConnection = System.getenv("DB_CONNECTION").strip();
    private static final String dbUser = System.getenv("DB_USER");
    private static final String dbPass = System.getenv("DB_PASS");
    private static final String dbName = System.getenv("DB_NAME");
    private static final String projectId = System.getenv("GOOGLE_CLOUD_PROJECT");

    private DataSource connectionPool;
    private int jobId, chunkId;
    private String prefix, filename;
    private BufferedReader br;
    private int count = 0, lengthLongest = 0;
    private String longestPal = "";

    @Override
    public void accept(GCSEvent event, Context context) throws SQLException, IOException {
        filename = event.name;
        logger.info("Processing file: " + filename);

        if (!filename.contains("/r0")) {
            logger.info("Palindrome counting only applies to r0 files");
            return;
        }
        prepareFields(filename);
        connectionPool = getMySqlConnectionPool();
        prepareJob();
        countPalindromes();
        updateCounts();
    }

    private void countPalindromes() throws IOException {
        StorageOptions options = StorageOptions.newBuilder().setProjectId(projectId).build();
        Storage storage = options.getService();
        Blob blob = storage.get(inputBucket, filename);
        ReadChannel readChannel = blob.reader();
        br = new BufferedReader(Channels.newReader(readChannel, "UTF-8"));

        String line;
        while ((line = br.readLine()) != null) {
            String[] words = line.split(" ");
            for (int i = 0; i < words.length; i++) {
                if (isPalindrome(words[i])) {
                    count++;
                    if (words[i].length() > lengthLongest) {
                        longestPal = words[i];
                        lengthLongest = longestPal.length();
                    }
                }
            }
        }
    }

    private boolean isPalindrome(String str) {
        for (int i = 0, j = str.length() - 1; i < j; i++, j--) {
            if (str.charAt(i) != str.charAt(j)) {
                return false;
            }
        }
        return true;
    }

    private void prepareJob() {
        try {
            String query = "SELECT id FROM `job` "
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

    private void updateCounts() throws SQLException {
        //
        int success = executeUpdate(
                "UPDATE `job` "
                        + "SET `numPals` = " + count + " , "
                        + "    `pal` = " + (lengthLongest < 100 ? longestPal : longestPal.substring(0, 100)) + " "
                        + "WHERE `id` = " + jobId + ";");

        logger.info("Finishing Job:" + jobId);
        if (success > 0) {
            logger.info("Completed counting palindromes in file: " + filename);
        }
    }

    private void prepareFields(String sortedFilename) {
        String[] firstSplit = sortedFilename.split("/");
        prefix = firstSplit[0];
        String filename = firstSplit[1];
        String[] fields = filename.split("_");
        chunkId = Integer.parseInt(fields[2].substring(0, fields[2].indexOf(".")));
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
            }
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
