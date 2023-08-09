package src._00_Database_Table_Creator;

import utility.Helper;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;


public class Database_Table_Creator {

    // query to download tpch.lineitem table from database
    private static final String query_base = "select ORDERKEY, PARTKEY, SUPPKEY, LINENUMBER from tpch.LINEITEM LIMIT ";

    // stores total number of rows of the table that need to be processed
    private static int totalRows;

    // stores the number threads and number of rows per thread that program should run on. Currently, this code supports only single level threading
    private static int numThreads;
    private static int numRowsPerThread;

    // variables to stores output details
    private static boolean showDetails = false;
    private static final ArrayList<Long> threadTimes = new ArrayList<>();

    // stores database connection variable
    private static Connection con;

    // writers for writing into files
    private static FileWriter writer1;
    private static FileWriter writer2;
    private static FileWriter writer3;
    private static FileWriter writer4;

    // helper variables
    private static int origORD;
    private static int origPART;
    private static int origSUPP;
    private static int origLINE;

    // constructor to initialize the object paramters
    public Database_Table_Creator(final int totalRows, final int numThreads, final boolean showDetails) {
        Database_Table_Creator.totalRows = totalRows;
        Database_Table_Creator.numThreads = numThreads;
        Database_Table_Creator.numRowsPerThread = totalRows/numThreads;
        Database_Table_Creator.showDetails = showDetails;
    }

    /**
     * Downloads Cleartext 'tpch.lineitem' table from database into 'data/cleartext folder'. It then creates four
     * additive and multiplicative shares of the table stored into 'data/shares folder'.
     *
     * @param args: takes number of rows to be processed as input
     */
    public static void main(String[] args) throws IOException {

        Database_Table_Creator DBShareCreation = new Database_Table_Creator(Integer.parseInt(args[0]), 1, false);

        doPreWork();

        // doWork lcreates and starts all threads
        doWork();

        // Write to file
        doPostWork();
    }

    /**
     * Performs initialization tasks like connecting to the database, writer objects.
     *
     * @throws IOException : throws during creation of directories
     */
    private static void doPreWork() throws IOException {

        // making connection to the database
        System.out.println("Connection Started");

        try {
            Database_Table_Creator.con = Helper.getConnection();
        } catch (SQLException ex) {
            System.out.println(ex);
        }

        // creating directories to stores shares and cleartext files
        System.out.println("Preparing CSV Files");

        String diskPath = "data/shares/";

        Files.createDirectories(Paths.get(diskPath));

        // initializing writer object for file writes
        try {
            writer1 = new FileWriter(diskPath + "ServerTable1.csv");
            writer2 = new FileWriter(diskPath + "ServerTable2.csv");
            writer3 = new FileWriter(diskPath + "ServerTable3.csv");
            writer4 = new FileWriter(diskPath + "ServerTable4.csv");
        } catch (IOException ex) {
            Logger.getLogger(Database_Table_Creator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     *  Downloads the cleartext tpch.lineitem tables and creates four shares of the same.
     * @return the time taken to download cleartext and create shares
      */
    private static long doWork() {

        // initializing thread list for storing all threads
        List<Thread> threadList = new ArrayList<>(); // The list containing all the threads

        long avgOperationTime = 0;

        // create threads and add them to thread list
        for(int i = 0; i < numThreads; i++){
            int threadNum = i+1;
            threadList.add(new Thread(new ParallelTask(numRowsPerThread, threadNum), "Thread" + threadNum));
        }

        // start all threads in the list
        for(int i = 0; i < numThreads; i++){
            threadList.get(i).start();
        }

        // wait for all threads to finish
        for (Thread thread : threadList) {
            try{
                thread.join();
            }catch(InterruptedException ex){
                System.out.println(ex.getMessage());
            }
        }

        /*
        The Thread run times for each thread are stored in the array threadTimes. This loop calculates the average
        thread time across all the threads
         */
        for (int i = 0; i < threadTimes.size(); i++) {
            avgOperationTime += threadTimes.get(i);
        }


        return(avgOperationTime/numThreads);

    }

    // defines work performed by each thread
    private static class ParallelTask implements Runnable {

        private final int numRows;
        private final int threadNum;

        public ParallelTask(final int numRows, final int threadNum) {
            this.numRows = numRows;
            this.threadNum = threadNum;
        }

        @Override
        public void run() {

            Instant threadStartTime = Instant.now();

            int count = 0;

            // start count of the row
            int startRow = (threadNum - 1) * numRows;

            // random object for random number generation needed while shares creation
            Random rand = new Random();

            // stores the result returned by database
            ResultSet rs;

            Instant startTime = Instant.now();

            try{

                // downloads the tpch.lineitem table from the database
                System.out.println("Table Loading Started");
                Instant tableLoad = Instant.now();

                String query = query_base + startRow + ", " + numRows;
                Statement stmt = con.createStatement();
                rs = stmt.executeQuery(query);

                long tableLoadTime = Duration.between(tableLoad, Instant.now()).toMillis();

                System.out.println("Splitting Started");
                Instant splittingStart = Instant.now();

                // for each row, each column value's shares both additive and multiplicative is created
                for(int i = startRow; i < startRow + numRows; i++){
                    rs.next();

                    origORD = rs.getInt("ORDERKEY");
                    origPART= rs.getInt("PARTKEY");
                    origSUPP = rs.getInt("SUPPKEY");
                    origLINE = rs.getInt("LINENUMBER");

                    /*
                    SUPPKEY: as 'supkey' is considered as string column, the additive shares of each digit of 'suppkey'
                    value is created
                     */
                    LinkedList<Integer> stack = new LinkedList<>();
                    ArrayList<Integer> numInArrList = new ArrayList<>();

                    ArrayList<Integer> c3p1LIST = new ArrayList<>();
                    ArrayList<Integer> c3p2LIST = new ArrayList<>();

                    int tempSUPP = origSUPP;

                    // iterating over digits
                    while (tempSUPP > 0) {
                        stack.push(tempSUPP % 10 );
                        tempSUPP = tempSUPP / 10;
                    }
                    while (!stack.isEmpty())
                        numInArrList.add(stack.pop());

                    /*
                     creating additive shares of each digit by generating a random number and using that as first part
                     and second part as the subtracted value between digit and random value.
                     */
                    for (Integer numPart : numInArrList)
                        c3p1LIST.add(rand.nextInt(100));

                    for (int j = 0; j < c3p1LIST.size(); j++)
                        c3p2LIST.add(numInArrList.get(j) - c3p1LIST.get(j));

                    // the additive shares of each digit is appended using '|'
                    String c01p1 = c3p1LIST.stream().map(Object::toString).collect(Collectors.joining("|"));
                    String c01p2 = c3p2LIST.stream().map(Object::toString).collect(Collectors.joining("|"));
                    String c01p3 = c01p1;
                    String c01p4 = c01p2;


                    /*
                     Orderkey: As this column is numeric hence creating additive shares of each value by generating a
                     random number and using that as first part and second part as the subtracted value between value
                     and random value.
                     */
                    int c02p1 = Helper.mod(rand.nextInt(1000000));
                    int c02p2 = origORD - c02p1;
                    int c02p3 = c02p1;
                    int c02p4 = c02p2;


                   /*
                     Partkey: As this column is numeric hence creating additive shares of each value by generating a
                     random number and using that as first part and second part as the subtracted value between value
                     and random value.
                     */
                    int c03p1 = Helper.mod(rand.nextInt(1000000));
                    int c03p2 = origPART - c03p1;
                    int c03p3 = c03p1;
                    int c03p4 = c03p2;


                    /*
                     Linenumber: As this column is numeric hence creating additive shares of each value by generating a
                     random number and using that as first part and second part as the subtracted value between value
                     and random value.
                     */
                    int c04p1 = Helper.mod(rand.nextInt(1000000));
                    int c04p2 = origLINE - c04p1;
                    int c04p3 = c04p1;
                    int c04p4 = c04p2;

                    /*
                     Orderkey: As this column is numeric hence creating multiplicative shares of each value by using
                     Shamir Secret Sharing function.
                     */
                    int ordVals[] = func(origORD);
                    int c08p1 = ordVals[0];
                    int c08p2 = ordVals[1];
                    int c08p3 = ordVals[2];
                    int c08p4 = ordVals[3];


                    /*
                     Partkey: As this column is numeric hence creating multiplicative shares of each value by using
                     Shamir Secret Sharing function.
                     */
                    int partVals[] = func(origPART);
                    int c09p1 = partVals[0];
                    int c09p2 = partVals[1];
                    int c09p3 = partVals[2];
                    int c09p4 = partVals[3];


                    /*                */
                    int lineVals[] = func(origLINE);
                    int c10p1 = lineVals[0];
                    int c10p2 = lineVals[1];
                    int c10p3 = lineVals[2];
                    int c10p4 = lineVals[3];

                    // M_Suppkey
                    int suppVals[] = func(origSUPP);
                    int c11p1 = suppVals[0];
                    int c11p2 = suppVals[1];
                    int c11p3 = suppVals[2];
                    int c11p4 = suppVals[3];


                    try {



                        writer1.append(String.valueOf(c04p1));
                        writer1.append(",");
                        writer1.append(String.valueOf(c03p1));
                        writer1.append(",");
                        writer1.append(String.valueOf(c02p1));
                        writer1.append(",");
                        writer1.append(c01p1);
                        writer1.append(",");
                        writer1.append(String.valueOf(c10p1));
                        writer1.append(",");
                        writer1.append(String.valueOf(c09p1));
                        writer1.append(",");
                        writer1.append(String.valueOf(c08p1));
                        writer1.append(",");
                        writer1.append(String.valueOf(c11p1));
                        writer1.append("\n");






                        writer2.append(String.valueOf(c04p2));
                        writer2.append(",");
                        writer2.append(String.valueOf(c03p2));
                        writer2.append(",");
                        writer2.append(String.valueOf(c02p2));
                        writer2.append(",");
                        writer2.append(c01p2);
                        writer2.append(",");
                        writer2.append(String.valueOf(c10p2));
                        writer2.append(",");
                        writer2.append(String.valueOf(c09p2));
                        writer2.append(",");
                        writer2.append(String.valueOf(c08p2));
                        writer2.append(",");
                        writer2.append(String.valueOf(c11p2));
                        writer2.append("\n");




                        writer3.append(String.valueOf(c04p3));
                        writer3.append(",");
                        writer3.append(String.valueOf(c03p3));
                        writer3.append(",");
                        writer3.append(String.valueOf(c02p3));
                        writer3.append(",");
                        writer3.append(c01p3);
                        writer3.append(",");
                        writer3.append(String.valueOf(c10p3));
                        writer3.append(",");
                        writer3.append(String.valueOf(c09p3));
                        writer3.append(",");
                        writer3.append(String.valueOf(c08p3));
                        writer3.append(",");
                        writer3.append(String.valueOf(c11p3));
                        writer3.append("\n");


                        writer4.append(String.valueOf(c04p4));
                        writer4.append(",");
                        writer4.append(String.valueOf(c03p4));
                        writer4.append(",");
                        writer4.append(String.valueOf(c02p4));
                        writer4.append(",");
                        writer4.append(c01p4);
                        writer4.append(",");
                        writer4.append(String.valueOf(c10p4));
                        writer4.append(",");
                        writer4.append(String.valueOf(c09p4));
                        writer4.append(",");
                        writer4.append(String.valueOf(c08p4));
                        writer4.append(",");
                        writer4.append(String.valueOf(c11p4));
                        writer4.append("\n");

                    } catch (IOException ex) {
                        Logger.getLogger(Database_Table_Creator.class.getName()).log(Level.SEVERE, null, ex);
                    }

                    if(i%(totalRows/100) == 0){
                        double percent = 100 * ((double)i/(double)totalRows);
                        Helper.progressBar(percent, Duration.between(startTime, Instant.now()).toMillis());
                    }
                }

                long tableSplittingTime = Duration.between(splittingStart, Instant.now()).toMillis();

                System.out.println("100% complete");

                System.out.println();
                System.out.println("Table Loading Time: " + tableLoadTime + " ms");
                System.out.println("Table Splitting Time: " + tableSplittingTime + " ms");


            } catch (SQLException ex) {
                System.out.println(ex);
            }

            Duration totalThreadTime = Duration.between(threadStartTime, Instant.now());

            if(showDetails) {
                System.out.println("\n" + Thread.currentThread().getName().toUpperCase());
                System.out.println("Total Operations: " + count);
                System.out.println("Total Thread Time: " + totalThreadTime.toMillis() + " ms");
                System.out.println("Thread Start Time: " + DateTimeFormatter.ofPattern("hh:mm:ss.SSS").format(LocalDateTime.ofInstant(threadStartTime, ZoneOffset.UTC)));
                System.out.println("Thread Start Time: " + DateTimeFormatter.ofPattern("hh:mm:ss.SSS").format(LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)));
            }

            threadTimes.add(totalThreadTime.toMillis());
        }
    }

    private static void doPostWork(){
        try {
            writer1.toString();
            writer1.flush();
            writer1.close();

            writer2.toString();
            writer2.flush();
            writer2.close();

            writer3.toString();
            writer3.flush();
            writer3.close();

            writer4.toString();
            writer4.flush();
            writer4.close();
        } catch (IOException ex) {
            Logger.getLogger(Database_Table_Creator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static int[] func(int b){
        Random rand = new Random();

        int m = rand.nextInt(2000) - 1000;

        int m1 = 1 * m + b;
        int m2 = 2 * m + b;
        int m3 = 3 * m + b;
        int m4 = 4 * m + b;

        int vals[] = new int[]{Helper.mod(m1), Helper.mod(m2), Helper.mod(m3), Helper.mod(m4)};

        return vals;
    }
}




