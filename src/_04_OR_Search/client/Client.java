package src._04_OR_Search.client;

import constant.Constants;
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
    private static String server3IP;
    private static int server3Port;
    private static String server4IP;
    private static int server4Port;
    private static int clientPort;
    private static String combinerIP;
    private static int combinerPort;

    private static String[] columnName;
    private static int[] columnValue;
    private static int columnCount;
    private static int[][] multiplicativeShares;
    private static int seedClient;

    private static int numRows;
    private static int numThreads;
    private static int numRowsPerThread;

    private static int[][] resultCombiner;
    private static final Set<Integer> result = Collections.synchronizedSet(new HashSet<Integer>());

    private static final ArrayList<Instant> timestamps1 = new ArrayList<>();
    private static final ArrayList<Instant> timestamps2 = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    private static final String resultFileName = "_04_OR_Search";


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
            Random randClient = new Random(seedClient);

            // Evaluating which rows matches the requested query
            for (int i = startRow; i < endRow; i++) {
                int randSeedClient = randClient.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                        + Constants.getMinRandomBound();
                if (resultCombiner[0][i] == randSeedClient) {
                    result.add(i + 1);
                }
                if (resultCombiner.length > 1) {
                    if (resultCombiner[1][i] == randSeedClient) {
                        result.add(i + 1);
                    }
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
                resultCombiner = (int[][]) inFromServer.readObject();
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
            socket.close();
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

    private static int[] helper(int index) {
        int[] data = new int[columnCount];
        for (int j = 0; j < columnCount; j++) {
            data[j] = multiplicativeShares[j][index];
        }
        return data;
    }

    private static void doPostWork() {
        int numServers;
        Client server1, server2, server3 = null, server4 = null, combiner;

        if (columnCount > 3) {
            numServers = 3;
        } else {
            numServers = columnCount + 1;
        }

        String[] data;

        data = new String[]{String.valueOf(numServers)};
        combiner = new Client(combinerIP, combinerPort, data);

        data = new String[]{Helper.strArrToStr(columnName), Helper.arrToStr(helper(0)), String.valueOf(seedClient)};
        server1 = new Client(server1IP, server1Port, data);

        data = new String[]{Helper.strArrToStr(columnName), Helper.arrToStr(helper(1)), String.valueOf(seedClient)};
        server2 = new Client(server2IP, server2Port, data);

        if (numServers > 2) {
            data = new String[]{Helper.strArrToStr(columnName), Helper.arrToStr(helper(2)), String.valueOf(seedClient)};
            server3 = new Client(server3IP, server3Port, data);
        }

        if (numServers > 3) {
            data = new String[]{Helper.strArrToStr(columnName), Helper.arrToStr(helper(3)), String.valueOf(seedClient)};
            server4 = new Client(server4IP, server4Port, data);
        }


        combiner.start();
        server1.start();
        server2.start();

        if (numServers > 2) {
            server3.start();
        }

        if (numServers > 3) {
            server4.start();
        }

        timestamps1.add(Instant.now());
        Client client = new Client();
        client.startAsReceiver();
    }

    private static int[] shamirSecretSharing(int value, int serverCount) {
        Random rand = new Random(1);
        int coefficient = rand.nextInt(2);
        int[] share = new int[serverCount];
        for (int i = 0; i < serverCount; i++) {
            share[i] = (i + 1) * coefficient + value;
        }
        return share;
    }

    private static void doWork() {

        if (columnCount > 3) {
            for (int i = 0; i < columnCount; i++)
                multiplicativeShares[i] = shamirSecretSharing(columnValue[i], columnCount - 1);
        } else {
            for (int i = 0; i < columnCount; i++)
                multiplicativeShares[i] = shamirSecretSharing(columnValue[i], columnCount + 1);
        }
    }

    private static void doPreWork(String[] args) {

        String query = args[0];

        String[] querySplit = query.split(",");
        columnCount = querySplit.length / 2;
        columnName = new String[columnCount];
        columnValue = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columnName[i] = "m_"+querySplit[2 * i];
            columnValue[i] = Integer.parseInt(querySplit[2 * i + 1]);
        }

        String pathName = "config/Client.properties";
        Properties properties = Helper.readPropertiesFile(pathName);

        seedClient = Integer.parseInt(properties.getProperty("seedClient"));

        numRows = Integer.parseInt(properties.getProperty("numRows"));
        numThreads = Integer.parseInt(properties.getProperty("numThreads"));
        numRowsPerThread = numRows / numThreads;

        clientPort = Integer.parseInt(properties.getProperty("clientPort"));
        server1IP = properties.getProperty("server1IP");
        server1Port = Integer.parseInt(properties.getProperty("server1Port"));
        server2IP = properties.getProperty("server2IP");
        server2Port = Integer.parseInt(properties.getProperty("server2Port"));
        server3IP = properties.getProperty("server3IP");
        server3Port = Integer.parseInt(properties.getProperty("server3Port"));
        server4IP = properties.getProperty("server4IP");
        server4Port = Integer.parseInt(properties.getProperty("server4Port"));
        combinerIP = properties.getProperty("combinerIP");
        combinerPort = Integer.parseInt(properties.getProperty("combinerPort"));

        int resultDim = 1;
        if (columnCount > 3)
            resultDim = 2;
        resultCombiner = new int[resultDim][numRows];
        multiplicativeShares = new int[columnCount][columnCount + 1];
    }

    public static void main(String[] args) throws InterruptedException {
        timestamps1.add(Instant.now());

        doPreWork(args);

        doWork();

        doPostWork();
    }
}


