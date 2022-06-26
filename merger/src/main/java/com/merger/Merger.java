package com.merger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
import com.merger.Merger.GCSEvent;
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

    private static final int chunkSize = 1024;
    private static final byte[] lineFeed = { '\n' };

    private DataSource connectionPool;
    private int round, leftId, rightId, jobId;
    private String prefix, leftFileName, rightFilename, outFilename;
    private boolean ready = false;
    private BufferedReader leftBr, rightBr;
    private List<String> leftList = new ArrayList<String>(), rightList = new ArrayList<String>(),
            outList = new ArrayList<String>();
    private BlobInfo outputInfo;
    private WriteChannel writer;

    @Override
    public void accept(GCSEvent event, Context context) throws SQLException, IOException {
        logger.info("Processing file: " + event.name);

        prepareFields(event.name); // Get necessary fields and check if pair file is ready
        if (!ready) {
            logger.info("When pair is ready, processing will start");
            return;
        }

        connectionPool = getMySqlConnectionPool();
        prepareJob();
        markJobInProgress();
        mergeFiles();
        markJobDone();
    }

    private void mergeFiles() throws IOException {
        StorageOptions options = StorageOptions.newBuilder().setProjectId(projectId).build();
        Storage storage = options.getService();
        Blob leftBlob = storage.get(inputBucket, leftFileName);
        Blob rightBlob = storage.get(inputBucket, rightFilename);
        ReadChannel leftReadChannel = leftBlob.reader();
        ReadChannel rightReadChannel = rightBlob.reader();
        leftBr = new BufferedReader(Channels.newReader(leftReadChannel, "UTF-8"));
        rightBr = new BufferedReader(Channels.newReader(rightReadChannel, "UTF-8"));
        logger.info("Merging into file: " + outFilename);
        BlobId blobId = BlobId.of(inputBucket, outFilename);
        outputInfo = BlobInfo.newBuilder(blobId).build();
        writer = storage.writer(outputInfo);

        // Fill lists from files
        fillList(leftList, leftBr);
        fillList(rightList, rightBr);

        while (!leftList.isEmpty() && !rightList.isEmpty()) {
            if (leftList.get(leftList.size() - 1).compareTo(rightList.get(rightList.size() - 1)) >= 0) {
                // last item in left is bigger, right will empty out first
                int leftIndex = 0;
                String leftLine = leftList.get(leftIndex);
                for (String rightLine : rightList) {
                    if (rightLine.compareTo(leftLine) <= 0) {
                        outList.add(rightLine);
                        continue;
                    }
                    do {
                        outList.add(leftLine);
                        leftIndex++;
                        leftLine = leftList.get(leftIndex);
                    } while (leftLine.compareTo(rightLine) < 0);
                    outList.add(rightLine);
                }
                rightList.clear();
                leftList.subList(0, leftIndex).clear();
                flushList(outList); // write to output file
                fillList(rightList, rightBr); // refill empty list
            } else {
                // Duplicate of above inverse, may use reference based method call to refactor
                int rightIndex = 0;
                String rightLine = rightList.get(rightIndex);
                for (String leftLine : leftList) {
                    if (leftLine.compareTo(rightLine) <= 0) {
                        outList.add(leftLine);
                        continue;
                    }
                    do {
                        outList.add(rightLine);
                        rightIndex++;
                        rightLine = rightList.get(rightIndex);
                    } while (rightLine.compareTo(leftLine) < 0);
                    outList.add(leftLine);
                }
                leftList.clear();
                rightList.subList(0, rightIndex).clear();
                flushList(outList); // write to output file
                fillList(leftList, leftBr); // refill empty list
            }
        }

        while (!leftList.isEmpty()) {
            flushList(leftList);
            fillList(leftList, leftBr);
        }
        while (!rightList.isEmpty()) {
            flushList(rightList);
            fillList(rightList, rightBr);
        }
        writer.close();
    }

    private void flushList(List<String> list) throws IOException {
        byte[] outBytes = (String.join("\n", list)).getBytes();
        int writtenBytes = writer.write(ByteBuffer.wrap(outBytes, 0, outBytes.length)) + 1;
        writer.write(ByteBuffer.wrap(lineFeed, 0, lineFeed.length));
        logger.info("Flushing to file: " + outFilename + " with length of " + writtenBytes + "bytes");
        list.clear();
    }

    private void fillList(List<String> list, BufferedReader br) throws IOException {
        String line;
        int size = 0;
        while (size < chunkSize && (line = br.readLine()) != null) {
            list.add(line);
            size += line.length();
        }
    }

    private void prepareJob() {
        try {
            String query = "SELECT id, chunk_two FROM `job` "
                    + "WHERE `prefix` LIKE '" + prefix + "' "
                    + "AND `type` LIKE 'merge_r" + round + "' "
                    + "AND `chunk_one` LIKE '" + leftFileName + "'"
                    // + "AND `chunk_two` LIKE '" + rightFilename +"'"
                    + "LIMIT 1;";
            logger.info("Query:" + query);
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
            logger.info("Merging files -> left:" + leftFileName + " , right:" + rightFilename);
        }
    }

    private void markJobDone() throws SQLException {
        int success = executeUpdate(
                "UPDATE `job` "
                        + "SET `status` = 'done' "
                        + "WHERE `id` = " + jobId + ";");

        logger.info("Marked Job:" + jobId + " done");
        if (success > 0) {
            logger.info("Completed merging files -> left:" + leftFileName + " , right:" + rightFilename);
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
        outFilename = prefix + "/r" + (round + 1) + "_chunk_" + (leftId / 2) + ".txt";
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
