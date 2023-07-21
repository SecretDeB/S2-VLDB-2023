package src._05_Multiplicative_Row_Fetch.server;

import constant.Constants;
import utility.Helper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server1 {

    private static final String query_base = "select M_ORDERKEY, M_PARTKEY, M_LINENUMBER, M_SUPPKEY from "
            + Helper.getTablePrefix() + "SERVERTABLE1 where rowID > ";


    private static int numRows;
    private static int numThreads;
    private static int numRowsPerThread;

    private static int[][][] orderKeySum;
    private static int[][][] partKeySum;
    private static int[][][] lineNumberSum;
    private static int[][][] subKeySum;
    private static int[] totalOrderKey;
    private static int[] totalPartKey;
    private static int[] totalLineNumberKey;
    private static int[] totalSubKey;
    private static int querySize;
    private static int filter_size;
    private static int[][] row_filter;
    private static int[][] col_filter;
    private static int seedClient;


    private static int[][] result;
    private static ArrayList<Instant> timestamps = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    private static int serverPort;
    private static int combinerPort;
    private static String combinerIP;

    private static class ParallelTask implements Runnable {

        private final int threadNum;

        public ParallelTask(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            Connection con = null;

            try {
                con = Helper.getConnection();
            } catch (SQLException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }

            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            try {

                String query = query_base + startRow + " LIMIT " + numRowsPerThread;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(query);

                for (int i = startRow; i < endRow; i++) {
                    rs.next();
                    for (int k = 0; k < querySize; k++) {
                        orderKeySum[k][threadNum - 1][i % filter_size] = (int) Helper.mod(orderKeySum[k][threadNum - 1][i % filter_size] + Helper.mod(rs.getLong("M_ORDERKEY") * row_filter[k][i / filter_size]));
                        partKeySum[k][threadNum - 1][i % filter_size] = (int) Helper.mod(partKeySum[k][threadNum - 1][i % filter_size] + Helper.mod(rs.getLong("M_PARTKEY") * row_filter[k][i / filter_size]));
                        lineNumberSum[k][threadNum - 1][i % filter_size] = (int) Helper.mod(lineNumberSum[k][threadNum - 1][i % filter_size] + Helper.mod(rs.getLong("M_LINENUMBER") * row_filter[k][i / filter_size]));
                        subKeySum[k][threadNum - 1][i % filter_size] = (int) Helper.mod(subKeySum[k][threadNum - 1][i % filter_size] + Helper.mod(rs.getLong("M_SUPPKEY") * row_filter[k][i / filter_size]));

                    }
                }

                for (int i = 0; i < querySize; i++) {
                    for (int k = 0; k < filter_size; k++) {
                        orderKeySum[i][threadNum - 1][k] = (int) Helper.mod(orderKeySum[i][threadNum - 1][k] * (long) col_filter[i][k]);
                        partKeySum[i][threadNum - 1][k] = (int) Helper.mod(partKeySum[i][threadNum - 1][k] * (long) col_filter[i][k]);
                        lineNumberSum[i][threadNum - 1][k] = (int) Helper.mod(lineNumberSum[i][threadNum - 1][k] * (long) col_filter[i][k]);
                        subKeySum[i][threadNum - 1][k] = (int) Helper.mod(subKeySum[i][threadNum - 1][k] * (long) col_filter[i][k]);

                        totalOrderKey[i] = (int) Helper.mod(totalOrderKey[i] + (long) orderKeySum[i][threadNum - 1][k]);
                        totalPartKey[i] = (int) Helper.mod(totalPartKey[i] + (long) partKeySum[i][threadNum - 1][k]);
                        totalLineNumberKey[i] = (int) Helper.mod(totalLineNumberKey[i] + (long) lineNumberSum[i][threadNum - 1][k]);
                        totalSubKey[i] = (int) Helper.mod(totalSubKey[i] + (long) subKeySum[i][threadNum - 1][k]);

                    }
                }
            } catch (SQLException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
            try {
                con.close();
            } catch (SQLException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    private static void doWork(String[] data) {

        row_filter = Helper.strToStrArr1(data[0]);
        col_filter = Helper.strToStrArr1(data[1]);
        seedClient = Integer.parseInt(data[2]);

        querySize = row_filter.length;
        result = new int[querySize + 1][4];

        // To store result for each thread upon column-wise multiply operation
        orderKeySum = new int[querySize][numThreads][filter_size];
        partKeySum = new int[querySize][numThreads][filter_size];
        lineNumberSum = new int[querySize][numThreads][filter_size];
        subKeySum = new int[querySize][numThreads][filter_size];

        // To store the result upon column-wise and row-wise multiplication
        totalOrderKey = new int[querySize];
        totalPartKey = new int[querySize];
        totalLineNumberKey = new int[querySize];
        totalSubKey = new int[querySize];

        // The list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // Create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask(threadNum), "Thread" + threadNum));
        }

        // Start all threads
        for (int i = 0; i < numThreads; i++) {
            threadList.get(i).start();
        }

        // Wait for all threads to finish
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }

        Random randSeedClient = new Random(seedClient);

        for (int i = 0; i < querySize; i++) {
            int randClient = randSeedClient.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                    + Constants.getMinRandomBound();
            result[i][0] = (int) Helper.mod(totalOrderKey[i] + (long) randClient);
            result[i][1] = (int) Helper.mod(totalPartKey[i] + (long) randClient);
            result[i][2] = (int) Helper.mod(totalLineNumberKey[i] + (long) randClient);
            result[i][3] = (int) Helper.mod(totalSubKey[i] + (long) randClient);
        }

        result[querySize][0] = 1;
    }

    static class SocketCreation {

        private final Socket clientSocket;


        SocketCreation(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        public void run() {
            ObjectInputStream inFromClient;
            Socket combinerSocket;
            ObjectOutputStream outToCombiner;
            String[] dataReceived;

            try {
                //Reading the data sent by Client
                inFromClient = new ObjectInputStream(clientSocket.getInputStream());
                dataReceived = (String[]) inFromClient.readObject();
                doWork(dataReceived);

                //Sending the processed data to Combiner
                combinerSocket = new Socket(combinerIP, combinerPort);
                outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());
                outToCombiner.writeObject(result);
                combinerSocket.close();

                //Calculating timestamps
                timestamps.add(Instant.now());
//                System.out.println(Helper.getProgramTimes(timestamps));
//                log.log(Level.INFO, "Total Server1 time:" + Helper.getProgramTimes(timestamps));
            } catch (IOException | ClassNotFoundException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    private void startServer() throws IOException {
        Socket socket;

        try {
            ServerSocket ss = new ServerSocket(serverPort);
            System.out.println("Server1 Listening........");

            do {
                socket = ss.accept();
                timestamps = new ArrayList<>();
                timestamps.add(Instant.now());
                new SocketCreation(socket).run();
            } while (true);
        } catch (IOException ex) {
            log.log(Level.SEVERE, ex.getMessage());
        }
    }

    private static void doPreWork() {

        String pathName = "config/Server1.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        serverPort = Integer.parseInt(properties.getProperty("serverPort"));
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort"));
        combinerIP = properties.getProperty("combinerIP");

        filter_size = (int) Math.sqrt(numRows);
    }

    public static void main(String[] args) throws IOException {

        doPreWork();

        Server1 server1 = new Server1();
        server1.startServer();

    }
}


