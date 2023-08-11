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

public class Server2 {

    // query string to get server data from database
    private static final String query_base = "select M_ORDERKEY, M_PARTKEY, M_LINENUMBER, M_SUPPKEY from "
            + Helper.getTablePrefix() + "SERVERTABLE2 where rowID > ";


    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads server program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;

    // store value for orderkey column of tpch.lineitem
    private static int[][][] orderKeySum;
    // store value for partkey column of tpch.lineitem
    private static int[][][] partKeySum;
    // store value for linenumber column of tpch.lineitem
    private static int[][][] lineNumberSum;
    // store value for suppkey column of tpch.lineitem
    private static int[][][] subKeySum;
    // store aggregate thread value for orderkey column of tpch.lineitem
    private static int[] totalOrderKey;
    // store aggregate thread value for partkey column of tpch.lineitem
    private static int[] totalPartKey;
    // store aggregate thread value for linenumber column of tpch.lineitem
    private static int[] totalLineNumberKey;
    // store aggregate thread value for suppkey column of tpch.lineitem
    private static int[] totalSubKey;
    // the total number of row ids requested
    private static int querySize;
    // the size of filter based on number of rows considered which is sqrt(numRows)
    private static int filter_size;
    // stores the row filter for row ids value
    private static int[][] row_filter;
    // stores the column filter for row ids value
    private static int[][] col_filter;
    // stores seed value for client for random number generation
    private static int seedClient;

    // stores result after server processing
    private static int[][] result;
    private static ArrayList<Instant> timestamps = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    // stores port for server
    private static int serverPort;
    // stores port for combiner
    private static int combinerPort;
    // stores IP for combiner
    private static String combinerIP;

    // operation performed by each thread
    private static class ParallelTask implements Runnable {

        private final int threadNum;

        public ParallelTask(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            // making connection to the database

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

                // performing server operation on each row of the database
                for (int i = startRow; i < endRow; i++) {
                    rs.next();
                    // multiplication with the row filter for each column value
                    for (int k = 0; k < querySize; k++) {
                        orderKeySum[k][threadNum - 1][i % filter_size] = (int) Helper.mod(orderKeySum[k][threadNum - 1][i % filter_size] + Helper.mod(rs.getLong("M_ORDERKEY") * row_filter[k][i / filter_size]));
                        partKeySum[k][threadNum - 1][i % filter_size] = (int) Helper.mod(partKeySum[k][threadNum - 1][i % filter_size] + Helper.mod(rs.getLong("M_PARTKEY") * row_filter[k][i / filter_size]));
                        lineNumberSum[k][threadNum - 1][i % filter_size] = (int) Helper.mod(lineNumberSum[k][threadNum - 1][i % filter_size] + Helper.mod(rs.getLong("M_LINENUMBER") * row_filter[k][i / filter_size]));
                        subKeySum[k][threadNum - 1][i % filter_size] = (int) Helper.mod(subKeySum[k][threadNum - 1][i % filter_size] + Helper.mod(rs.getLong("M_SUPPKEY") * row_filter[k][i / filter_size]));
                    }
                }

                for (int i = 0; i < querySize; i++) {
                    // multiplication with the col filter for each column value
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

    // executing server operation over threads
    private static void doWork(String[] data) {

        row_filter = Helper.strToStrArr1(data[0]);
        col_filter = Helper.strToStrArr1(data[1]);
        seedClient = Integer.parseInt(data[2]);

        querySize = row_filter.length;
        result = new int[querySize + 1][4];

        // to store result for each thread upon column-wise multiply operation
        orderKeySum = new int[querySize][numThreads][filter_size];
        partKeySum = new int[querySize][numThreads][filter_size];
        lineNumberSum = new int[querySize][numThreads][filter_size];
        subKeySum = new int[querySize][numThreads][filter_size];

        // to store the result upon column-wise and row-wise multiplication
        totalOrderKey = new int[querySize];
        totalPartKey = new int[querySize];
        totalLineNumberKey = new int[querySize];
        totalSubKey = new int[querySize];

        // the list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // Create threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask(threadNum), "Thread" + threadNum));
        }

        // start all threads
        for (int i = 0; i < numThreads; i++) {
            threadList.get(i).start();
        }

        // wait for all threads to finish
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }

        Random randSeedClient = new Random(seedClient);
        // adding random value before sending to Client
        for (int i = 0; i < querySize; i++) {
            int randClient = randSeedClient.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                    + Constants.getMinRandomBound();
            result[i][0] = (int) Helper.mod(totalOrderKey[i] + (long) randClient);
            result[i][1] = (int) Helper.mod(totalPartKey[i] + (long) randClient);
            result[i][2] = (int) Helper.mod(totalLineNumberKey[i] + (long) randClient);
            result[i][3] = (int) Helper.mod(totalSubKey[i] + (long) randClient);
        }

        result[querySize][0] = 2;
    }

    // performing operations on data received over socket
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
                // reading the data sent by Client
                inFromClient = new ObjectInputStream(clientSocket.getInputStream());
                dataReceived = (String[]) inFromClient.readObject();
                doWork(dataReceived);

                // sending the processed data to Combiner
                combinerSocket = new Socket(combinerIP, combinerPort);
                outToCombiner = new ObjectOutputStream(combinerSocket.getOutputStream());
                outToCombiner.writeObject(result);
                combinerSocket.close();

                // calculating timestamps
                timestamps.add(Instant.now());
//                System.out.println(Helper.getProgramTimes(timestamps));
//                log.log(Level.INFO, "Total Server2 time:" + Helper.getProgramTimes(timestamps));
            } catch (IOException | ClassNotFoundException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // starting server to listening for incoming connection
    private void startServer() throws IOException {
        Socket socket;

        try {
            ServerSocket ss = new ServerSocket(serverPort);
            System.out.println("Server2 Listening........");

            do {
                // listening over socket for connections
                socket = ss.accept();
                timestamps = new ArrayList<>();
                timestamps.add(Instant.now());
                new SocketCreation(socket).run();
            } while (true);
        } catch (IOException ex) {
            log.log(Level.SEVERE, ex.getMessage());
        }
    }

    /**
     * It performs initialization tasks
     */
    private static void doPreWork() {

        // reads configuration properties of the server
        String pathName = "config/Server2.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        serverPort = Integer.parseInt(properties.getProperty("serverPort"));
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort"));
        combinerIP = properties.getProperty("combinerIP");

        filter_size = (int) Math.sqrt(numRows);
    }

    // performs server task required to process client query
    public static void main(String[] args) throws IOException {

        doPreWork();

        Server2 Server2 = new Server2();
        Server2.startServer();

    }
}


