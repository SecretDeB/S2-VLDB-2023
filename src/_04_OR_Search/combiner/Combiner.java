package src._04_OR_Search.combiner;

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
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Combiner extends Thread {
    private static List<int[][]> serverResult = Collections.synchronizedList(new ArrayList<>());
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(Constants.getThreadPoolSize());
    private static List<SocketCreation> socketCreations = new ArrayList<>();
    private static int[][] result;

    private static int numRows;
    private static int numThreads;
    private static int numRowsPerThread;

    private static int combinerPort;
    private static int clientPort;
    private static String clientIP;

    private static int[][] server1;
    private static int[][] server2;
    private static int[][] server3;
    private static int[][] server4;
    private static int serverCount;
    private static boolean flag = true;

    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static ArrayList<Instant> timestamps = new ArrayList<>();

    private static int langrangesInterpolatation(int share[]) {
        return switch (share.length) {
            case 2 -> (int) Helper.mod(Helper.mod((long) 2 * share[0]) - share[1]);
            case 3 -> (int) Helper.mod(Helper.mod((long) 3 * share[0]) - Helper.mod((long) 3 * share[1]) + share[2]);
            case 4 ->
                    (int) Helper.mod(Helper.mod(Helper.mod(((long) 4 * share[0])) - (Helper.mod((long) 6 * share[1])) + (Helper.mod((long) 4 * share[2])) - (Helper.mod(share[3]))));
            default -> 0;
        };
    }

    private static class ParallelTask implements Runnable {
        private int threadNum;

        public ParallelTask(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;

            // The operation of adding each of the M1 and M2 values

            int[] share1 = null, share2;
            for (int i = startRow; i < endRow; i++) {
                if (server1.length > 2) {
                    share1 = new int[]{server1[0][i], server2[0][i], server3[0][i]};
                    share2 = new int[]{server1[1][i], server2[1][i], server3[1][i]};
                    result[0][i] = langrangesInterpolatation(share1);
                    result[1][i] = langrangesInterpolatation(share2);
                } else {
                    switch (serverCount) {
                        case 2 -> share1 = new int[]{server1[0][i], server2[0][i]};
                        case 3 -> share1 = new int[]{server1[0][i], server2[0][i], server3[0][i]};
                        case 4 -> share1 = new int[]{server1[0][i], server2[0][i], server3[0][i], server4[0][i]};
                    }
                    result[0][i] = langrangesInterpolatation(share1);
                }
            }
        }
    }

    private static void doWork() {
        // The list containing all the threads


        for (int i = 0; i < serverResult.size(); i++) {
            switch (serverResult.get(i)[serverResult.get(i).length - 1][0]) {
                case 1 -> server1 = serverResult.get(i);
                case 2 -> server2 = serverResult.get(i);
                case 3 -> server3 = serverResult.get(i);
                case 4 -> server4 = serverResult.get(i);
            }
        }

        int resultDim = 1;
        if (server1.length > 2)
            resultDim = 2;
        result = new int[resultDim][numRows];

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
                ex.printStackTrace();
            }
        }
    }

    class SocketCreation implements Runnable {

        private final Socket serverSocket;

        SocketCreation(Socket serverSocket) {
            this.serverSocket = serverSocket;
        }

        @Override
        public void run() {
            ObjectInputStream inFromServer;
            try {
                inFromServer = new ObjectInputStream(serverSocket.getInputStream());
                serverResult.add((int[][]) inFromServer.readObject());
                serverSocket.close();
            } catch (IOException ex) {
                Logger.getLogger(Combiner.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void run() {
        startCombiner();
        super.run();
    }

    private void startCombiner() {
        Socket serverSocket;
        Socket clientSocket;
        ArrayList<Future> serverJobs = new ArrayList<>();

        try {
            ServerSocket ss = new ServerSocket(combinerPort);
            System.out.println("Combiner Listening........");

            while (true) {
                //Reading data from the server
                serverSocket = ss.accept();

                if (flag) {
                    ObjectInputStream inFromServer = new ObjectInputStream(serverSocket.getInputStream());
                    serverCount= Integer.parseInt(((String[])inFromServer.readObject())[0]);
                    flag = false;
                    serverSocket.close();
                } else {
                    socketCreations.add(new SocketCreation(serverSocket));
                }

                //Processing data received from both the servers
                if (socketCreations.size() == serverCount) {
                    timestamps = new ArrayList<>();
                    timestamps.add(Instant.now());
                    for (SocketCreation socketCreation : socketCreations) {
                        serverJobs.add(threadPool.submit(socketCreation));
                    }
                    for (Future<?> future : serverJobs)
                        future.get();
                    doWork();
                    //Sending data from the client

                    clientSocket = new Socket(clientIP, clientPort);
                    ObjectOutputStream outToClient = new ObjectOutputStream(clientSocket.getOutputStream());
                    outToClient.writeObject(result);
                    clientSocket.close();

                    //Resetting storage variables
                    serverJobs = new ArrayList<>();
                    serverResult = Collections.synchronizedList(new ArrayList<>());
                    socketCreations = new ArrayList<>();
                    flag = true;

                    //Calculating the time spent
                    timestamps.add(Instant.now());
//                    System.out.println(Helper.getProgramTimes(timestamps));
//                  log.log(Level.INFO, "Total Combiner time:" + Helper.getProgramTimes(timestamps));
                }
            }
        } catch (IOException | ExecutionException | InterruptedException | ClassNotFoundException ex) {
            log.log(Level.SEVERE, ex.getMessage());
        }
    }

    private static void doPreWork(String[] args) {
        //Reading Combiner property file
        String pathName = "config/Combiner.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        clientPort = Integer.parseInt(properties.getProperty("clientPort"));
        clientIP = properties.getProperty("clientIP");
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort"));
    }

    public static void main(String args[]) {

        doPreWork(args);

        Combiner combiner = new Combiner();
        combiner.startCombiner();

    }
}