package src._02_oneColumnNumberSearch.combiner;

import constant.*;
import utility.Helper;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Combiner extends Thread {
    // stores result received from servers
    private static List<int[]> serverResult = Collections.synchronizedList(new ArrayList<>());
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(Constants.getThreadPoolSize());
    private static List<SocketCreation> socketCreations = new ArrayList<>();
    private static int[] result;

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads combiner program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;

    // stores port value for combiner
    private static int combinerPort;
    // stores port value for client
    private static int clientPort;
    // stores IP value for client
    private static String clientIP;

    // stores server data
    private static int[] server1;
    private static int[] server2;

    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static ArrayList<Instant> timestamps = new ArrayList<>();

    // operation performed by each thread
    private static class ParallelTask implements Runnable {
        private int threadNum;

        public ParallelTask(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;
            // adding data received from the server
            for (int i = startRow; i < endRow; i++) {
                result[i] = (int) Helper.mod((long) server1[i] + (long) server2[i]);
            }
        }
    }

    // working on server data to process for client
    private static void doWork() {
        // the list containing all the threads

        server1 = serverResult.get(0);
        server2 = serverResult.get(1);
        List<Thread> threadList = new ArrayList<>();

        // create threads and add them to threadlist
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
    }

    // socket to read data from servers
    class SocketCreation implements Runnable {

        private final Socket serverSocket;

        SocketCreation(Socket serverSocket) {
            this.serverSocket = serverSocket;
        }

        @Override
        public void run() {
            ObjectInputStream inFromServer;
            try {
                // initializing input stream for reading the data
                inFromServer = new ObjectInputStream(serverSocket.getInputStream());
                serverResult.add((int[]) inFromServer.readObject());
            } catch (IOException | ClassNotFoundException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    @Override
    public void run() {
        startCombiner();
        super.run();
    }

    // starting combiner to process server data
    private void startCombiner() {
        Socket serverSocket;
        Socket clientSocket;
        ArrayList<Future> serverJobs = new ArrayList<>();

        try {
            ServerSocket ss = new ServerSocket(combinerPort);
            System.out.println("Combiner Listening........");

            while (true) {
                // listening over the socket for connections
                serverSocket = ss.accept();
                // reading data from the server
                socketCreations.add(new SocketCreation(serverSocket));

                // processing data after receiving data from both the servers
                if (socketCreations.size() == 2) {
                    timestamps = new ArrayList<>();
                    timestamps.add(Instant.now());
                    for (SocketCreation socketCreation : socketCreations) {
                        serverJobs.add(threadPool.submit(socketCreation));
                    }
                    for (Future<?> future : serverJobs)
                        future.get();
                    doWork();
                    // sending data from the client
                    clientSocket = new Socket(clientIP, clientPort);
                    ObjectOutputStream outToClient = new ObjectOutputStream(clientSocket.getOutputStream());
                    outToClient.writeObject(result);
                    clientSocket.close();

                    // resetting storage variables
                    result = new int[numRows];
                    serverJobs = new ArrayList<>();
                    serverResult = Collections.synchronizedList(new ArrayList<>());
                    socketCreations = new ArrayList<>();

                    // calculating the time spent
                    timestamps.add(Instant.now());
//                    System.out.println(Helper.getProgramTimes(timestamps));
//                    log.log(Level.INFO, "Total Combiner time:" + Helper.getProgramTimes(timestamps));
                }
            }
        } catch (IOException | ExecutionException | InterruptedException ex) {
            log.log(Level.SEVERE, ex.getMessage());
        }
    }

    /**
     * It performs initialization tasks
     */
    private static void doPreWork(String[] args) {
        // reading combiner property file
        String pathName = "config/Combiner.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        clientPort = Integer.parseInt(properties.getProperty("clientPort"));
        clientIP = properties.getProperty("clientIP");
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort"));

        result = new int[numRows];
    }

    /**
     * combiner process the data received from server before sending to client
     */
    public static void main(String args[]) {

        doPreWork(args);

        Combiner combiner = new Combiner();
        combiner.startCombiner();

    }
}