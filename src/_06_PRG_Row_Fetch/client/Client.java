package src._06_PRG_Row_Fetch.client;

import constant.Constants;
import src._05_Multiplicative_Row_Fetch.combiner.Combiner;
import utility.Helper;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class Client extends Thread {

    // stores IP value of desired server/client
    public String IP;
    // stores port value of desired server/client
    public int port;
    // stores data send out by the client to servers
    private String[] data;


    // the number of row of tpch.lineitem considered
    public static int numRows;

    // stores IP for server1
    private static String server1IP;
    // stores port for server1
    private static int server1Port;
    // stores IP for server2
    private static String server2IP;
    // stores port for server2
    private static int server2Port;
    // stores IP for server3
    private static String server3IP;
    // stores port for server3
    private static int server3Port;
    // stores IP for server4
    private static String server4IP;
    // stores port for server4
    private static int server4Port;
    // stores IP for client
    private static int clientPort;

    // list of all row ids requested
    private static int[] queryList;
    // number of  row ids requested
    private static int querySize;
    // based on number of rows as sqrt(numRows)
    private static int filter_size;
    // creating block vector for server
    private static int[][] blockVec1;
    private static int[][] blockVec2;
    // creating seed vector for server
    private static int[][] seedArr1;
    private static int[][] seedArr2;
    // creating row vector for server
    private static int[][] rowVec1;
    private static int[][] rowVec2;

    private static final List<SocketCreation> socketCreations = new ArrayList<>();
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(Constants.getThreadPoolSize());
    private static final List<int[][][]> serverResult = Collections.synchronizedList(new ArrayList<>());

    // used to calculate the time taken by client program
    private static final ArrayList<Instant> timestamps1 = new ArrayList<>();
    private static ArrayList<Instant> timestamps2 = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    // for storing data send to each server
    private static int[][][] server1;
    private static int[][][] server2;
    private static int[][][] server3;
    private static int[][][] server4;

    // stores the result
    private static int[][] result;
    // the name of file storing the query result under result/ folder
    private static final String resultFileName = "_06_PRG_Row_Fetch";


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
    private static void interpolation() throws IOException {

        StringBuilder stringBuilder;
        // extracting server data
        for (int i = 0; i < serverResult.size(); i++) {
            switch (serverResult.get(i)[serverResult.get(i).length - 1][0][0]) {
                case 1 -> server1 = serverResult.get(i);
                case 2 -> server2 = serverResult.get(i);
                case 3 -> server3 = serverResult.get(i);
                case 4 -> server4 = serverResult.get(i);
            }
        }
        // evaluating which rows matches the requested query and storing row ids in result list
        for (int i = 0; i < querySize; i++) {
            result[i][0] = Helper.mod(server1[i][0][0] - server2[i][0][0] - server3[i][0][0] + server4[i][0][0]);
            result[i][1] = Helper.mod(server1[i][1][0] - server2[i][1][0] - server3[i][1][0] + server4[i][1][0]);
            result[i][2] = Helper.mod(server1[i][2][0] - server2[i][2][0] - server3[i][2][0] + server4[i][2][0]);

            for (int j = 0; j < Constants.getNumberSize(); j++) {
                result[i][3] = (int) Helper.mod(result[i][3] + Helper.mod((Helper.mod(server1[i][3][j] - server2[i][3][j] - server3[i][3][j] + server4[i][3][j])) * (long) (Math.pow(10, j))));
            }
            stringBuilder = new StringBuilder();
            result[i][3] = Integer.parseInt(String.valueOf(stringBuilder.append(result[i][3]).reverse()));

        }

        Helper.printResult(result, queryList, resultFileName);
    }

    // receiving server data over the socket
    class SocketCreation implements Runnable {

        private final Socket serverSocket;

        SocketCreation(Socket serverSocket) {
            this.serverSocket = serverSocket;
        }

        @Override
        public void run() {
            ObjectInputStream inFromServer;
            try {
                // Receiving the data from the server
                inFromServer = new ObjectInputStream(serverSocket.getInputStream());
                serverResult.add((int[][][]) inFromServer.readObject());
            } catch (IOException ex) {
                Logger.getLogger(Combiner.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // starting to listen for incoming responses from servers
    private void startAsReceiver() {
        Socket socket;
        ArrayList<Future> serverJobs = new ArrayList<>();

        try {
            ServerSocket ss = new ServerSocket(clientPort);
            System.out.println("Client Listening........");

            while (true) {
                // reading data from the server
                socket = ss.accept();
                socketCreations.add(new SocketCreation(socket));

                // processing data received from both the servers
                if (socketCreations.size() == 4) {
                    timestamps2 = new ArrayList<>();
                    timestamps2.add(Instant.now());
                    for (SocketCreation socketCreation : socketCreations) {
                        serverJobs.add(threadPool.submit(socketCreation));
                    }
                    for (Future<?> future : serverJobs)
                        future.get();
                    interpolation();

                    // calculating the time spent
                    timestamps2.add(Instant.now());
                    int totalTime = Math.toIntExact(Helper.getProgramTimes(timestamps1).get(0)) +
                            Math.toIntExact(Helper.getProgramTimes(timestamps2).get(0));
//                    System.out.println(totalTime);
//                    log.log(Level.INFO, "Total Client time:" + totalTime);
                    System.exit(0);
                }
            }
        } catch (IOException | ExecutionException | InterruptedException ex) {
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
        Client server1, server2, server3, server4;

        // server data preparation
        String[] data;
        data = new String[]{Helper.arrToStr(rowVec1), Helper.arrToStr(rowVec2), Helper.arrToStr(seedArr1), Helper.arrToStr(blockVec1)};
        server1 = new Client(server1IP, server1Port, data);

        data = new String[]{Helper.arrToStr(rowVec1), Helper.arrToStr(rowVec2), Helper.arrToStr(seedArr2), Helper.arrToStr(blockVec2)};
        server2 = new Client(server2IP, server2Port, data);

        data = new String[]{Helper.arrToStr(rowVec1), Helper.arrToStr(rowVec2), Helper.arrToStr(seedArr2), Helper.arrToStr(blockVec2)};
        server3 = new Client(server3IP, server3Port, data);

        data = new String[]{Helper.arrToStr(rowVec1), Helper.arrToStr(rowVec2), Helper.arrToStr(seedArr1), Helper.arrToStr(blockVec1)};
        server4 = new Client(server4IP, server4Port, data);

        // sending data to each server
        server1.start();
        server2.start();
        server3.start();
        server4.start();

        // started to listen for incoming responses from servers
        timestamps1.add(Instant.now());
        Helper.getProgramTimes(timestamps1);
        Client client = new Client();
        client.startAsReceiver();
    }

    /**
     * It performs initialization tasks
     */
    public static void doWork() {
        Random random = new Random(1);
        int[][] R1 = new int[querySize][filter_size];
        int[][] R2 = new int[querySize][filter_size];

        int temp, rowNumber, columnNumber;
        // extracting row and col for each row ids
        for (int i = 0; i < querySize; i++) {
            rowNumber = queryList[i] / filter_size;
            columnNumber = queryList[i] % filter_size;

            // making filter for those row and col
            for (int j = 0; j < filter_size; j++) {
                temp = random.nextInt(2);
                blockVec1[i][j] = temp;
                blockVec2[i][j] = temp;

                temp = random.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                        + Constants.getMinRandomBound();
                seedArr1[i][j] = temp;
                seedArr2[i][j] = temp;

                temp = random.nextInt(2);
                R1[i][j] = temp;
                R2[i][j] = temp;

                if (j == rowNumber) {
                    blockVec1[i][j] = 1;
                    blockVec2[i][j] = 0;
                    seedArr2[i][j] += 1;
                }

                if (j == columnNumber) {
                    R1[i][j] = 1;
                    R2[i][j] = 0;
                }
            }

            Random randomSeedPart1 = new Random(seedArr1[i][rowNumber]);
            Random randomSeedPart2 = new Random(seedArr2[i][rowNumber]);

            for (int k = 0; k < filter_size; k++) {
                int a = randomSeedPart1.nextInt(2);
                int b = randomSeedPart2.nextInt(2);
                rowVec1[i][k] = a ^ R1[i][k];
                rowVec2[i][k] = b ^ R2[i][k];
            }
        }
    }

    /**
     * It performs initialization tasks
     * @param args takes as string a list of row ids e.g. "1,2,5,6"
     */
    private static void doPreWork(String[] args) {

        String query = args[0];

        // splitting the argument value to extract row ids to be searched
        queryList = Stream.of(query.split(","))
                .mapToInt(Integer::parseInt).map(i -> i - 1)
                .toArray();

        querySize = queryList.length;

        // reads configuration properties of the client
        String pathName = "config/Client.properties";
        Properties properties = Helper.readPropertiesFile(pathName);


        numRows = Integer.parseInt(properties.getProperty("numRows"));

        clientPort = Integer.parseInt(properties.getProperty("clientPort"));
        server1IP = properties.getProperty("server1IP");
        server1Port = Integer.parseInt(properties.getProperty("server1Port"));
        server2IP = properties.getProperty("server2IP");
        server2Port = Integer.parseInt(properties.getProperty("server2Port"));
        server3IP = properties.getProperty("server3IP");
        server3Port = Integer.parseInt(properties.getProperty("server3Port"));
        server4IP = properties.getProperty("server4IP");
        server4Port = Integer.parseInt(properties.getProperty("server4Port"));

        filter_size = (int) Math.sqrt(numRows);

        blockVec1 = new int[querySize][filter_size];
        blockVec2 = new int[querySize][filter_size];
        seedArr1 = new int[querySize][filter_size];
        seedArr2 = new int[querySize][filter_size];
        rowVec1 = new int[querySize][filter_size];
        rowVec2 = new int[querySize][filter_size];

        result = new int[querySize][4];
    }

    /**
     * This program is used to retrieve the records corresponding to requested row ids based on additive shares.
     *
     * @param args takes as string a list of row ids e.g. "1,2,5,6"
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        timestamps1.add(Instant.now());

        doPreWork(args);

        doWork();

        doPostWork();
    }
}


