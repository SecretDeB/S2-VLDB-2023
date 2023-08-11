package src._05_Multiplicative_Row_Fetch.client;

import constant.Constants;
import utility.Helper;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.*;
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
    // stores seed value for client for random number generation
    private static int seedClient;
    // in square matrix form the row to which row ids belong
    private static int[][] row_filter;
    // in square matrix form the column to which row ids belong
    private static int[][] col_filter;
    // for each server the row shares for row ids values
    private static int[][] server1RowShare;
    private static int[][] server2RowShare;
    private static int[][] server3RowShare;
    private static int[][] server4RowShare;
    // for each server the column shares for row ids values
    private static int[][] server1ColShare;
    private static int[][] server2ColShare;
    private static int[][] server3ColShare;
    private static int[][] server4ColShare;

    // the number of row of tpch.lineitem considered
    private static int numRows;
    // stores the result received/sent from/to combiner
    private static int[][] resultCombiner;

    // used to calculate the time taken by client program
    private static final ArrayList<Instant> timestamps1 = new ArrayList<>();
    private static final ArrayList<Instant> timestamps2 = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    // the name of file storing the query result under result/ folder
    private static final String resultFileName = "_05_Multiplicative_Row_Fetch";


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
        querySize = resultCombiner.length;
        Random randSeedClient = new Random(seedClient);

        // evaluating which rows matches the requested query and storing row ids in result list
        for (int i = 0; i < querySize; i++) {
            int randClient = randSeedClient.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                    + Constants.getMinRandomBound();

            resultCombiner[i][0] = (int) Helper.mod(resultCombiner[i][0] - (long) randClient);
            resultCombiner[i][1] = (int) Helper.mod(resultCombiner[i][1] - (long) randClient);
            resultCombiner[i][2] = (int) Helper.mod(resultCombiner[i][2] - (long) randClient);
            resultCombiner[i][3] = (int) Helper.mod(resultCombiner[i][3] - (long) randClient);

            // print result for each row id
            Helper.printResult(resultCombiner, queryList, resultFileName);
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
                resultCombiner = (int[][]) inFromServer.readObject();
                // interpolating data to get results
                interpolation();
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
        Client server1, server2, server3, server4;

        // server data preparation
        String[] data;
        data = new String[]{Helper.arrToStr(server1RowShare), Helper.arrToStr(server1ColShare), String.valueOf(seedClient)};
        server1 = new Client(server1IP, server1Port, data);

        data = new String[]{Helper.arrToStr(server2RowShare), Helper.arrToStr(server2ColShare), String.valueOf(seedClient)};
        server2 = new Client(server2IP, server2Port, data);

        data = new String[]{Helper.arrToStr(server3RowShare), Helper.arrToStr(server3ColShare), String.valueOf(seedClient)};
        server3 = new Client(server3IP, server3Port, data);

        data = new String[]{Helper.arrToStr(server4RowShare), Helper.arrToStr(server4ColShare), String.valueOf(seedClient)};
        server4 = new Client(server4IP, server4Port, data);

        // sending data to each server
        server1.start();
        server2.start();
        server3.start();
        server4.start();

        // started to listen for incoming responses from servers
        timestamps1.add(Instant.now());
        Client client = new Client();
        client.startAsReceiver();
    }

    /**
     * The function creates multiplicative secret share using Shamir Secret Sharing
     *
     * @param value : the secret whose share is to be created
     * @param serverCount: the number of shares that is to be created based on number of servers
     * @return a list of shares of secret of length 'serverCount'
     */
    private static int[] shamirSecretSharing(int value, int serverCount) {
        Random rand = new Random();
        // storing the slope value for the line
        int coefficient = rand.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound()) +
                Constants.getMinRandomBound();
        // stores shares of the secret
        int[] share = new int[serverCount];
        // for value of x starting from 1, evaluates the share for  'value'
        for (int i = 0; i < serverCount; i++) {
            share[i] = (i + 1) * coefficient + value;
        }
        return share;
    }

    /**
     * It performs initialization tasks
     */
    private static void doWork() {

        // evaluating the row and column filter based on items to be searched
        int filter_size = (int) Math.sqrt(numRows);
        for (int i = 0; i < querySize; i++) {
            row_filter[i][queryList[i] / filter_size] = 1;
            col_filter[i][queryList[i] % filter_size] = 1;
        }

        // The number of servers is 4
        int serverCount = 4;
        int[] row_split;
        int[] col_split;

        // creating shares for row and column filter values
        for (int i = 0; i < querySize; i++) {
            for (int j = 0; j < row_filter[i].length; j++) {
                row_split = shamirSecretSharing(row_filter[i][j], serverCount);
                col_split = shamirSecretSharing(col_filter[i][j], serverCount);
                server1RowShare[i][j] = row_split[0];
                server1ColShare[i][j] = col_split[0];
                server2RowShare[i][j] = row_split[1];
                server2ColShare[i][j] = col_split[1];
                server3RowShare[i][j] = row_split[2];
                server3ColShare[i][j] = col_split[2];
                server4RowShare[i][j] = row_split[3];
                server4ColShare[i][j] = col_split[3];
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
        Properties properties = Helper.readPropertiesFile( pathName);

        seedClient = Integer.parseInt(properties.getProperty("seedClient"));

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

        int filter_size = (int) Math.sqrt(numRows);
        row_filter = new int[querySize][filter_size];
        col_filter = new int[querySize][filter_size];
        server1RowShare = new int[querySize][filter_size];
        server1ColShare = new int[querySize][filter_size];
        server2RowShare = new int[querySize][filter_size];
        server2ColShare = new int[querySize][filter_size];
        server3RowShare = new int[querySize][filter_size];
        server3ColShare = new int[querySize][filter_size];
        server4RowShare = new int[querySize][filter_size];
        server4ColShare = new int[querySize][filter_size];
    }

    /**
     * This program is used to retrieve the records corresponding to requested row ids based on multiplicative shares.
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


