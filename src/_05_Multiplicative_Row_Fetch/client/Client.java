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

    private static int[] queryList;
    private static int querySize;
    private static int seedClient;
    private static int[][] row_filter;
    private static int[][] col_filter;
    private static int[][] server1RowShare;
    private static int[][] server2RowShare;
    private static int[][] server3RowShare;
    private static int[][] server4RowShare;
    private static int[][] server1ColShare;
    private static int[][] server2ColShare;
    private static int[][] server3ColShare;
    private static int[][] server4ColShare;

    private static int numRows;
    private static int[][] resultCombiner;

    private static final ArrayList<Instant> timestamps1 = new ArrayList<>();
    private static final ArrayList<Instant> timestamps2 = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    private static final String resultFileName = "_05_Multiplicative_Row_Fetch";


    private Client() {
    }

    public Client(String IP, int port, String[] data) {
        this.IP = IP;
        this.port = port;
        this.data = data;
    }

    private static void interpolation() throws IOException {
        querySize = resultCombiner.length;
        Random randSeedClient = new Random(seedClient);

        for (int i = 0; i < querySize; i++) {
            int randClient = randSeedClient.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound())
                    + Constants.getMinRandomBound();

            resultCombiner[i][0] = (int) Helper.mod(resultCombiner[i][0] - (long) randClient);
            resultCombiner[i][1] = (int) Helper.mod(resultCombiner[i][1] - (long) randClient);
            resultCombiner[i][2] = (int) Helper.mod(resultCombiner[i][2] - (long) randClient);
            resultCombiner[i][3] = (int) Helper.mod(resultCombiner[i][3] - (long) randClient);

            Helper.printResult(resultCombiner, queryList, resultFileName);
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
        Client server1, server2, server3, server4;

        String[] data;
        data = new String[]{Helper.arrToStr(server1RowShare), Helper.arrToStr(server1ColShare), String.valueOf(seedClient)};
        server1 = new Client(server1IP, server1Port, data);

        data = new String[]{Helper.arrToStr(server2RowShare), Helper.arrToStr(server2ColShare), String.valueOf(seedClient)};
        server2 = new Client(server2IP, server2Port, data);

        data = new String[]{Helper.arrToStr(server3RowShare), Helper.arrToStr(server3ColShare), String.valueOf(seedClient)};
        server3 = new Client(server3IP, server3Port, data);

        data = new String[]{Helper.arrToStr(server4RowShare), Helper.arrToStr(server4ColShare), String.valueOf(seedClient)};
        server4 = new Client(server4IP, server4Port, data);

        server1.start();
        server2.start();
        server3.start();
        server4.start();

        timestamps1.add(Instant.now());
        Client client = new Client();
        client.startAsReceiver();
    }

    private static int[] shamirSecretSharing(int value, int serverCount) {
        Random rand = new Random();
        int coefficient = rand.nextInt(Constants.getMaxRandomBound() - Constants.getMinRandomBound()) +
                Constants.getMinRandomBound();
        int[] share = new int[serverCount];
        for (int i = 0; i < serverCount; i++) {
            share[i] = (i + 1) * coefficient + value;
        }
        return share;
    }

    private static void doWork() {

        // Evaluating the row and column filter based on items to be searched
        int filter_size = (int) Math.sqrt(numRows);
        for (int i = 0; i < querySize; i++) {
            row_filter[i][queryList[i] / filter_size] = 1;
            col_filter[i][queryList[i] % filter_size] = 1;
        }

        // The number of servers is 4
        int serverCount = 4;
        int[] row_split;
        int[] col_split;

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

    private static void doPreWork(String[] args) {
        String query = args[0];

        queryList = Stream.of(query.split(","))
                .mapToInt(Integer::parseInt).map(i -> i - 1)
                .toArray();

        querySize = queryList.length;

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

    public static void main(String[] args) throws InterruptedException {
        timestamps1.add(Instant.now());

        doPreWork(args);

        doWork();

        doPostWork();
    }
}


