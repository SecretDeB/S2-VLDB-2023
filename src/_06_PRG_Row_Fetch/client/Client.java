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

    public String IP;
    public int port;
    private String[] data;

    public static int numRows;

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
    private static int filter_size;
    private static int[][] blockVec1;
    private static int[][] blockVec2;
    private static int[][] seedArr1;
    private static int[][] seedArr2;
    private static int[][] rowVec1;
    private static int[][] rowVec2;


    private static final List<SocketCreation> socketCreations = new ArrayList<>();
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(Constants.getThreadPoolSize());
    private static final List<int[][][]> serverResult = Collections.synchronizedList(new ArrayList<>());


    private static final ArrayList<Instant> timestamps1 = new ArrayList<>();
    private static ArrayList<Instant> timestamps2 = new ArrayList<>();
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    private static int[][][] server1;
    private static int[][][] server2;
    private static int[][][] server3;
    private static int[][][] server4;

    private static int[][] result;
    private static final String resultFileName = "_06_PRG_Row_Fetch";


    private Client() {
    }

    public Client(String IP, int port, String[] data) {
        this.IP = IP;
        this.port = port;
        this.data = data;
    }


    private static void interpolation() throws IOException {

        StringBuilder stringBuilder;
        for (int i = 0; i < serverResult.size(); i++) {
            switch (serverResult.get(i)[serverResult.get(i).length - 1][0][0]) {
                case 1 -> server1 = serverResult.get(i);
                case 2 -> server2 = serverResult.get(i);
                case 3 -> server3 = serverResult.get(i);
                case 4 -> server4 = serverResult.get(i);
            }
        }

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
                serverResult.add((int[][][]) inFromServer.readObject());
            } catch (IOException ex) {
                Logger.getLogger(Combiner.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void startAsReceiver() {
        Socket socket;
        ArrayList<Future> serverJobs = new ArrayList<>();

        try {
            ServerSocket ss = new ServerSocket(clientPort);
            System.out.println("Client Listening........");

            while (true) {
                //Reading data from the server
                socket = ss.accept();
                socketCreations.add(new SocketCreation(socket));

                //Processing data received from both the servers
                if (socketCreations.size() == 4) {
                    timestamps2 = new ArrayList<>();
                    timestamps2.add(Instant.now());
                    for (SocketCreation socketCreation : socketCreations) {
                        serverJobs.add(threadPool.submit(socketCreation));
                    }
                    for (Future<?> future : serverJobs)
                        future.get();
                    interpolation();

                    //Calculating the time spent
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
        data = new String[]{Helper.arrToStr(rowVec1), Helper.arrToStr(rowVec2), Helper.arrToStr(seedArr1), Helper.arrToStr(blockVec1)};
        server1 = new Client(server1IP, server1Port, data);

        data = new String[]{Helper.arrToStr(rowVec1), Helper.arrToStr(rowVec2), Helper.arrToStr(seedArr2), Helper.arrToStr(blockVec2)};
        server2 = new Client(server2IP, server2Port, data);

        data = new String[]{Helper.arrToStr(rowVec1), Helper.arrToStr(rowVec2), Helper.arrToStr(seedArr2), Helper.arrToStr(blockVec2)};
        server3 = new Client(server3IP, server3Port, data);

        data = new String[]{Helper.arrToStr(rowVec1), Helper.arrToStr(rowVec2), Helper.arrToStr(seedArr1), Helper.arrToStr(blockVec1)};
        server4 = new Client(server4IP, server4Port, data);

        server1.start();
        server2.start();
        server3.start();
        server4.start();

        timestamps1.add(Instant.now());
        Helper.getProgramTimes(timestamps1);
        Client client = new Client();
        client.startAsReceiver();
    }

    public static void doWork() {
        Random random = new Random(1);
        int[][] R1 = new int[querySize][filter_size];
        int[][] R2 = new int[querySize][filter_size];

        int temp, rowNumber, columnNumber;
        for (int i = 0; i < querySize; i++) {
            rowNumber = queryList[i] / filter_size;
            columnNumber = queryList[i] % filter_size;

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

    private static void doPreWork(String[] args) {

        String query = args[0];

        queryList = Stream.of(query.split(","))
                .mapToInt(Integer::parseInt).map(i -> i - 1)
                .toArray();

        querySize = queryList.length;

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

    public static void main(String[] args) throws InterruptedException {
        timestamps1.add(Instant.now());

        doPreWork(args);

        doWork();

        doPostWork();
    }
}


