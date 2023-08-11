package src._06_PRG_Row_Fetch.server;

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
    private static final String query_base = "select ORDERKEY, PARTKEY, LINENUMBER, SUPPKEY from " +
            Helper.getTablePrefix() + "SERVERTABLE2 where rowID > ";

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads server program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;

    // storing block vector
    private static int[][] blockVec1;
    // storing row vector
    private static int[][] rowVec1;
    // storing row vector
    private static int[][] rowVec2;
    // storing seed vector
    private static int[][] seedArr1;
    // storing filter size
    private static int filter_size;


    // store value for orderkey column of tpch.lineitem
    private static int[][] orderKeySum;
    // store value for partkey column of tpch.lineitem
    private static int[][] partKeySum;
    // store value for linenumber column of tpch.lineitem
    private static int[][] lineNumberSum;
    // store value for subkey column of tpch.lineitem
    private static int[][][] subKeySum;
    // storing number of row ids requested
    private static int querySize;

    // stores result after server processing
    private static int[][][] result;
    private static ArrayList<Instant> timestamps = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    // stores port for server
    private static int serverPort;
    // stores port for client
    private static int clientPort;
    // stores IP for client
    private static String clientIP;

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

                int rowNumber;
                // performing server operation on each row of the database
                for (int i = startRow; i < endRow; i += filter_size) {
                    rowNumber = i / filter_size;

                    int[][] seedArrayGenerated = new int[querySize][filter_size];
                    for (int j = 0; j < querySize; j++) {
                        Random random = new Random(seedArr1[j][rowNumber]);
                        for (int k = 0; k < filter_size; k++) {
                            seedArrayGenerated[j][k] = random.nextInt(2);
                        }
                    }

                    for (int j = 0; j < filter_size; j++) {
                        rs.next();

                        for (int k = 0; k < querySize; k++) {
                            if (blockVec1[k][rowNumber] == 1) {
                                if ((rowVec1[k][j] ^ seedArrayGenerated[k][j]) == 1) {
                                    orderKeySum[k][threadNum - 1] = (int) Helper.mod(orderKeySum[k][threadNum - 1] + Helper.mod(rs.getLong("ORDERKEY")));
                                    partKeySum[k][threadNum - 1] = (int) Helper.mod(partKeySum[k][threadNum - 1] + Helper.mod(rs.getLong("PARTKEY")));
                                    lineNumberSum[k][threadNum - 1] = (int) Helper.mod(lineNumberSum[k][threadNum - 1] + Helper.mod(rs.getLong("LINENUMBER")));

                                    int[] temp = Helper.strToArr(rs.getString("SUPPKEY"));
                                    for (int l = 0; l < temp.length; l++) {
                                        subKeySum[k][threadNum - 1][l] = (int) Helper.mod(subKeySum[k][threadNum - 1][l] + (long) temp[l]);
                                    }
                                }
                            } else {
                                if ((rowVec2[k][j] ^ seedArrayGenerated[k][j]) == 1) {
                                    orderKeySum[k][threadNum - 1] = (int) Helper.mod(orderKeySum[k][threadNum - 1] + Helper.mod(rs.getLong("ORDERKEY")));
                                    partKeySum[k][threadNum - 1] = (int) Helper.mod(partKeySum[k][threadNum - 1] + Helper.mod(rs.getLong("PARTKEY")));
                                    lineNumberSum[k][threadNum - 1] = (int) Helper.mod(lineNumberSum[k][threadNum - 1] + Helper.mod(rs.getLong("LINENUMBER")));

                                    int[] temp = Helper.strToArr(rs.getString("SUPPKEY"));
                                    for (int l = 0; l < temp.length; l++) {
                                        subKeySum[k][threadNum - 1][l] = (int) Helper.mod(subKeySum[k][threadNum - 1][l] + (long) temp[l]);
                                    }
                                }
                            }
                        }
                    }
                }

                for (int i = 0; i < querySize; i++) {
                    result[i][0][0] = (int) Helper.mod(result[i][0][0] + (long) orderKeySum[i][threadNum - 1]);
                    result[i][1][0] = (int) Helper.mod(result[i][1][0] + (long) partKeySum[i][threadNum - 1]);
                    result[i][2][0] = (int) Helper.mod(result[i][2][0] + (long) lineNumberSum[i][threadNum - 1]);

                    for (int j = 0; j < Constants.getNumberSize(); j++) {
                        result[i][3][j] = (int) Helper.mod(result[i][3][j] + (long) subKeySum[i][threadNum - 1][j]);
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

        rowVec1 = Helper.strToStrArr1(data[0]);
        rowVec2 = Helper.strToStrArr1(data[1]);
        seedArr1 = Helper.strToStrArr1(data[2]);
        blockVec1 = Helper.strToStrArr1(data[3]);

        filter_size = rowVec1[0].length;
        querySize = rowVec1.length;

        result = new int[querySize + 1][4][Constants.getNumberSize()];

        // To store result for each thread upon column-wise multiply operation
        orderKeySum = new int[querySize][numThreads];
        partKeySum = new int[querySize][numThreads];
        lineNumberSum = new int[querySize][numThreads];
        subKeySum = new int[querySize][numThreads][Constants.getNumberSize()];

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

        result[querySize][0][0] = 2;
    }

    // performing operations on data received over socket
    static class SocketCreation {

        private final Socket clientSocketIn;


        SocketCreation(Socket clientSocketIn) {
            this.clientSocketIn = clientSocketIn;
        }

        public void run() {
            ObjectInputStream inFromClient;
            Socket clientSocketOut;
            ObjectOutputStream outToCombiner;
            String[] dataReceived;

            try {
                //Reading the data sent by Client
                inFromClient = new ObjectInputStream(clientSocketIn.getInputStream());
                dataReceived = (String[]) inFromClient.readObject();
                doWork(dataReceived);

                //Sending the processed data to Combiner
                clientSocketOut = new Socket(clientIP, clientPort);
                outToCombiner = new ObjectOutputStream(clientSocketOut.getOutputStream());
                outToCombiner.writeObject(result);
                clientSocketOut.close();

                //Calculating timestamps
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
        clientPort = Integer.parseInt(properties.getProperty("clientPort"));
        clientIP = properties.getProperty("clientIP");

        filter_size = (int) Math.sqrt(numRows);
    }

    // performs server task required to process client query
    public static void main(String[] args) throws IOException {

        doPreWork();

        Server2 Server2 = new Server2();
        Server2.startServer();

    }
}


