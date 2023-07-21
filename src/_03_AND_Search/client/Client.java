package src._03_AND_Search.client;

import constant.*;
import utility.Helper;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Client extends Thread {

    public String IP;
    public int port;
    private String[] data;


    private static String server1IP;
    private static int server1Port;
    private static String server2IP;
    private static int server2Port;
    private static int clientPort;

    private static String[] columnName;
    private static int[] columnValue;
    private static int columnCount;
    private static int fingerprintPrimeNumber;
    private static int fingerprint1;
    private static int fingerprint2;
    private static int seedClient;

    private static int numRows;
    private static int numThreads;
    private static int numRowsPerThread;

    private static int[] resultCombiner;
    private static final List<Integer> result = Collections.synchronizedList(new ArrayList<>());

    private static final ArrayList<Instant> timestamps1 = new ArrayList<>();
    private static final ArrayList<Instant> timestamps2 = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    private static final String resultFileName = "_03_AND_Search";


    private Client() {
    }

    public Client(String IP, int port, String[] data) {
        this.IP = IP;
        this.port = port;
        this.data = data;
    }

    private static class ParallelTask implements Runnable {
        private final int threadNum;

        public ParallelTask(int threadNum) {
            this.threadNum = threadNum;
        }

        @Override
        public void run() {
            int startRow = (threadNum - 1) * numRowsPerThread;
            int endRow = startRow + numRowsPerThread;
            Random randSeedClient = new Random(seedClient);
            int prg;

            // Evaluating which rows matches the requested query
            for (int i = startRow; i < endRow; i++) {
                prg = randSeedClient.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                        + Constants.getMinRandomBound();
                if (resultCombiner[i] == prg) {
                    result.add(i + 1);
                }
            }
        }
    }

    //
    private static void interpolation() {
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
    }

    static class ReceiverSocket {

        private Socket socket;

        ReceiverSocket(Socket socket) {
            this.socket = socket;
        }

        @SuppressWarnings("unchecked")
        public void run() {
            try {
                // Receiving the data from the Combiner
                ObjectInputStream inFromServer = new ObjectInputStream(socket.getInputStream());
                resultCombiner = (int[]) inFromServer.readObject();
                interpolation();
            } catch (IOException | ClassNotFoundException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    private void startAsReceiver() {
        Socket socket;

        try {
            ServerSocket ss = new ServerSocket(clientPort);
            System.out.println("Client Listening........");
            socket = ss.accept();
            timestamps2.add(Instant.now());

            new ReceiverSocket(socket).run();
            Helper.printResult(result, resultFileName);

            timestamps2.add(Instant.now());
            int totalTime = Math.toIntExact(Helper.getProgramTimes(timestamps1).get(0)) +
                    Math.toIntExact(Helper.getProgramTimes(timestamps2).get(0));
//            System.out.println(totalTime);
//            log.log(Level.INFO, "Total Client time:" + totalTime);
        } catch (IOException ex) {
            log.log(Level.SEVERE, ex.getMessage());
        }
    }

    private void startAsSender() {
        Socket socket;
        ObjectOutputStream outToServer;
        try {
            socket = new Socket(IP, port);
            outToServer = new ObjectOutputStream(socket.getOutputStream());
            outToServer.writeObject(data);
            socket.close();
        } catch (IOException ex) {
            log.log(Level.SEVERE, ex.getMessage());
        }
    }

    @Override
    public void run() {
        startAsSender();
        super.run();
    }

    private static void doPostWork() {
        String[] data;
        data = new String[]{Helper.strArrToStr(columnName), String.valueOf(fingerprint1), String.valueOf(seedClient)};
        Client server1 = new Client(server1IP, server1Port, data);

        data = new String[]{Helper.strArrToStr(columnName), String.valueOf(fingerprint2)};
        Client server2 = new Client(server2IP, server2Port, data);

        server1.start();
        server2.start();

        timestamps1.add(Instant.now());
        Client client = new Client();
        client.startAsReceiver();
    }

    private static void doWork() {
        Random rand = new Random();

        int start = 1;
        for (int i = 0; i < columnCount; i++) {
            int additiveShare1, additiveShare2, multiplier, prg;

            prg = rand.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                    + Constants.getMinRandomBound();

            if (columnName[i].equalsIgnoreCase("suppkey")) {
                int[] querySplit = Helper.stringToIntArray(String.valueOf(columnValue[i]));

                for (int j = 0; j < querySplit.length; j++) {
                    additiveShare1 = prg;
                    additiveShare2 = querySplit[j] - additiveShare1;
                    multiplier = (int) Helper.mod((long) Math.pow(fingerprintPrimeNumber, start));

                    fingerprint1 = (int) Helper.mod(fingerprint1 +
                            Helper.mod((long) multiplier * (long) additiveShare1));
                    fingerprint2 = (int) Helper.mod(fingerprint2 +
                            Helper.mod((long) multiplier * (long) additiveShare2));
                    start++;
                }
            } else {
                additiveShare1 = prg;
                additiveShare2 = columnValue[i] - additiveShare1;
                multiplier = (int) Helper.mod((long) Math.pow(fingerprintPrimeNumber, start));

                fingerprint1 = (int) Helper.mod(fingerprint1 +
                        Helper.mod((long) multiplier * (long) additiveShare1));
                fingerprint2 = (int) Helper.mod(fingerprint2 +
                        Helper.mod((long) multiplier * (long) additiveShare2));
                start++;
            }
        }
    }

    private static void doPreWork(String[] args) {
        String query = args[0];

        String[] querySplit = query.split(",");
        columnCount = querySplit.length / 2;
        columnName = new String[columnCount];
        columnValue = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columnName[i] = querySplit[2 * i];
            columnValue[i] = Integer.parseInt(querySplit[2 * i + 1]);
        }

        String pathName = "config/Client.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        seedClient = Integer.parseInt(properties.getProperty("seedClient"));
        fingerprintPrimeNumber = Integer.parseInt(properties.getProperty("fingerprintPrimeNumber"));

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        clientPort = Integer.parseInt(properties.getProperty("clientPort"));
        server1IP = properties.getProperty("server1IP");
        server1Port = Integer.parseInt(properties.getProperty("server1Port"));
        server2IP = properties.getProperty("server2IP");
        server2Port = Integer.parseInt(properties.getProperty("server2Port"));

        resultCombiner = new int[numRows];
    }

    public static void main(String[] args) throws InterruptedException {
        timestamps1.add(Instant.now());

        doPreWork(args);

        doWork();

        doPostWork();
    }
}


