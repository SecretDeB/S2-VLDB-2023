package src._05_Multiplicative_Row_Fetch.combiner;

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
    private static List<int[][]> serverResult = Collections.synchronizedList(new ArrayList<>());
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(Constants.getThreadPoolSize());
    private static List<SocketCreation> socketCreations = new ArrayList<>();
    private static int[][] result;

    // stores port value for combiner
    private static int combinerPort;
    // stores port value for client
    private static int clientPort;
    // stores IP value for client
    private static String clientIP;

    // stores server data
    private static int[][] server1;
    private static int[][] server2;
    private static int[][] server3;
    private static int[][] server4;
    private static int querySize;

    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static ArrayList<Instant> timestamps = new ArrayList<>();

    // shamir secret share data interpolation
    private static int langrangesInterpolatation(int share[]) {
        return switch (share.length) {
            case 2 -> (int) Helper.mod(Helper.mod((long) 2 * share[0]) - share[1]);
            case 3 -> (int) Helper.mod(Helper.mod((long) 3 * share[0]) - Helper.mod((long) 3 * share[1]) + share[2]);
            case 4 ->
                    (int) Helper.mod(Helper.mod((long) 4 * share[0]) - Helper.mod((long) 6 * share[1]) + Helper.mod((long) 4 * share[2]) - share[3]);
            default -> 0;
        };
    }

    // working on server data to process for client
    private static void doWork() {
        // extracting server based information
        for (int i = 0; i < serverResult.size(); i++) {
            switch (serverResult.get(i)[serverResult.get(i).length - 1][0]) {
                case 1 -> server1 = serverResult.get(i);
                case 2 -> server2 = serverResult.get(i);
                case 3 -> server3 = serverResult.get(i);
                case 4 -> server4 = serverResult.get(i);
            }
        }

        querySize = server1.length - 1;
        result = new int[querySize][4];

        // interpolating values from shares
        int[] share;
        for (int i = 0; i < querySize; i++) {
            share = new int[]{server1[i][0], server2[i][0], server3[i][0], server4[i][0]};
            result[i][0] = (langrangesInterpolatation(share));
            share = new int[]{server1[i][1], server2[i][1], server3[i][1], server4[i][1]};
            result[i][1] = (langrangesInterpolatation(share));
            share = new int[]{server1[i][2], server2[i][2], server3[i][2], server4[i][2]};
            result[i][2] = (langrangesInterpolatation(share));
            share = new int[]{server1[i][3], server2[i][3], server3[i][3], server4[i][3]};
            result[i][3] = (langrangesInterpolatation(share));
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
                serverResult.add((int[][]) inFromServer.readObject());
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

    // starting combiner to process server data
    private void startCombiner() {
        Socket serverSocket;
        Socket clientSocket;
        ArrayList<Future> serverJobs = new ArrayList<>();

        try {
            ServerSocket ss = new ServerSocket(combinerPort);
            System.out.println("Combiner Listening........");

            while (true) {
                // reading data from the server
                serverSocket = ss.accept();
                socketCreations.add(new SocketCreation(serverSocket));

                // processing data received from both the servers
                if (socketCreations.size() == 4) {
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
                    result = new int[querySize][4];
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
        // reading Combiner property file
        String pathName = "config/Combiner.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        clientPort = Integer.parseInt(properties.getProperty("clientPort"));
        clientIP = properties.getProperty("clientIP");
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort"));

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