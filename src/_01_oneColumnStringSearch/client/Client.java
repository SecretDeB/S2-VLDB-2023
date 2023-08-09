package src._01_oneColumnStringSearch.client;

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

    // stores IP value of desired server/client
    public String IP;
    // stores port value of desired server/client
    public int port;
    // stores data send out by the client to servers
    private String[] data;

    // stores IP for server1
    private static String server1IP;
    // stores port for server1
    private static int server1Port;
    // stores IP for server2
    private static String server2IP;
    // stores IP for server2
    private static int server2Port;
    // stores port for client
    private static int clientPort;

    // the name of the tpch.lineitem column to search over
    private static String columnName;
    // the value of the tpch.lineitem column to search over
    private static int columnValue;
    // the fingerprintPrimeNumber value i.e value of r which is taken as 43 in our case
    private static int fingerprintPrimeNumber;
    // the fingerprint value generated for server1
    private static int fingerprint1;
    // the fingerprint value generated for server2
    private static int fingerprint2;
    // stores seed value for client for random number generation
    private static int seedClient;

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // the number of threads client program is running on
    private static int numThreads;
    // the number of row per thread
    private static int numRowsPerThread;

    // stores the result received/sent from/to combiner
    private static int[] resultCombiner;
    // stores result received from servers
    private static final List<Integer> result = Collections.synchronizedList(new ArrayList<>());

    // used to calculate the time taken by client program
    private static final ArrayList<Instant> timestamps1 = new ArrayList<>();
    private static final ArrayList<Instant> timestamps2 = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    // the name of file storing the query result under result/ folder
    private static final String resultFileName = "_01_oneColumnStringSearch";


    // default constructor
    private Client() {
    }

    // parametrised constructor
    public Client(String IP, int port, String[] data) {
        this.IP = IP;
        this.port = port;
        this.data = data;
    }

    // operation performed by each thread
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

            // Evaluating which rows matches the requested query and storing row ids in result list
            for (int i = startRow; i < endRow; i++) {
                prg = randSeedClient.nextInt(Constants.getMaxRandomBound() -
                        Constants.getMinRandomBound()) + Constants.getMinRandomBound();
                if (resultCombiner[i] == prg) {
                    result.add(i + 1);
                }
            }
        }
    }

    // to interpolate the data received from the server
    private static void interpolation() {
        // the list containing all the threads
        List<Thread> threadList = new ArrayList<>();

        // wreate threads and add them to threadlist
        int threadNum;
        for (int i = 0; i < numThreads; i++) {
            threadNum = i + 1;
            threadList.add(new Thread(new ParallelTask(threadNum), "Thread" + threadNum));
        }

        // wtart all threads
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

    // receiving server data over the socket
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
                // interpolating data to get results
                interpolation();
                // socket closed
                socket.close();
            } catch (IOException | ClassNotFoundException ex) {
                log.log(Level.SEVERE, ex.getMessage());
            }
        }
    }

    // starting to listen for incoming responses from servers
    private void startAsReceiver() {
        Socket socket;

        try {
            ServerSocket ss = new ServerSocket(clientPort);
            System.out.println("Client Listening........");
            // listening over socket for incoming connections
            socket = ss.accept();
            timestamps2.add(Instant.now());

            // processing data received from server
            new ReceiverSocket(socket).run();

            // printing result of the query
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

    // to send client data to servers
    private void startAsSender() {
        Socket socket;
        ObjectOutputStream outToServer;
        try {
            // socket creation and initialising output stream to write data
            socket = new Socket(IP, port);
            outToServer = new ObjectOutputStream(socket.getOutputStream());
            // writing data to stream
            outToServer.writeObject(data);
            // socket closed
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

    // prepares data to send to server and starts listening to target servers
    private static void doPostWork() {

        // server data preparation
        String[] data;
        data = new String[]{columnName, String.valueOf(fingerprint1), String.valueOf(seedClient)};
        Client server1 = new Client(server1IP, server1Port, data);

        data = new String[]{columnName, String.valueOf(fingerprint2)};
        Client server2 = new Client(server2IP, server2Port, data);

        // sending data to each server
        server1.start();
        server2.start();

        // started to listen for incoming responses from servers
        timestamps1.add(Instant.now());
        Client client = new Client();
        client.startAsReceiver();
    }

    // creates additive shares of the search keyword value and generated fingerprint for each server
    private static void doWork() {
        Random random = new Random();

        int additiveShare1, additiveShare2, multiplier;
        // extracts each digit/letter of string value
        int[] valueSplit = Helper.stringToIntArray(String.valueOf(columnValue));

        // loops over each digit/letter to generate fingerprint value for server
        for (int j = valueSplit.length - 1; j >= 0; j--) {
            // additive share for digit/letter
            additiveShare1 = random.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                    + Constants.getMinRandomBound();
            additiveShare2 = valueSplit[j] - additiveShare1;

            // fingerprint generation
            multiplier = (int) Helper.mod((long) Math.pow(fingerprintPrimeNumber, j + 1));

            fingerprint1 = (int) Helper.mod(fingerprint1 +
                    Helper.mod((long) multiplier * (long) additiveShare1));
            fingerprint2 = (int) Helper.mod(fingerprint2 +
                    Helper.mod((long) multiplier * (long) additiveShare2));
        }
    }

    /**
     * It performs initialization tasks
     * @param args takes as string a column name and column value e.g. "suppkey,145"
     */

    private static void doPreWork(String[] args) {

        String query = args[0];

        // splitting the argument value to extract column name and value to be searched
        String[] querySplit = query.split(",");
        columnName = querySplit[0];
        columnValue = Integer.parseInt(querySplit[1]);

        // reads configuration properties of the client
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

    /**
     * This program is used to search a key over a string type column. It can search one string value at a time.
     *
     * @param args takes as string a column name and column value e.g. "suppkey,145"
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        timestamps1.add(Instant.now());

        doPreWork(args);

        doWork();

        doPostWork();
    }
}


