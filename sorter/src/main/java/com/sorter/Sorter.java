package com.sorter;

import com.sorter.event.PubSubMessage;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;



import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.logging.Logger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class Sorter implements BackgroundFunction<PubSubMessage> {
    private static Storage storage = StorageOptions.getDefaultInstance().getService();
    private static final Logger logger = Logger.getLogger(Sorter.class.getName());

    private static final String inputBucket = System.getenv("INPUT_BUCKET");
    private static final String dbConnection = System.getenv("DB_CONNECTION");
    private static final String dbUser = System.getenv("DB_USER");
    private static final String dbPass = System.getenv("DB_PASS");
    private static final String dbName = System.getenv("DB_NAME");
    private static final String projectId = System.getenv("GOOGLE_CLOUD_PROJECT");

    private DataSource connectionPool;
    private String chunkName, nextChunk;

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

    private void getChunkName(String prefix) {
        try {
            ResultSet results = executeQuery(
                    "SELECT * FROM `job`"
                            + "WHERE `prefix` LIKE '" + prefix + "' "
                            + "AND `type` LIKE 'quicksort' "
                            + "AND `status` LIKE `pending`"
                            + "LIMIT 1");

            int jobId = results.getInt("id");
            if (results.wasNull()) {
                // sorting is done nothing to do
                return;
            }

            int success = executeUpdate(
                    "UPDATE `job`"
                            + "SET `status` = 'in_progress'"
                            + "WHERE `id` = " + jobId);

            if (success > 0) {
                chunkName = results.getString("chunk_one");
                nextChunk = results.getString("chunk_two");
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

    private void sortChunk() {

        Blob inputBlob = storage.get(BlobId.of(inputBucket, chunkName));
        if (!inputBlob.exists()) {
            return;
        }

        // TODO: discard incomplete first line
        byte[] byteContent = storage.readAllBytes(inputBucket, chunkName);
        String content = new String(byteContent);

        // TODO: get first line from next blob
        // try ()

        String[] lines = content.split("\n");
        Arrays.sort(lines);

        content = String.join("\n", lines);

    }

    private void readChunk(ReadChannel reader, long size) throws IOException {

    }
}
