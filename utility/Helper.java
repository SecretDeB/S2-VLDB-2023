package utility;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class Helper {

    // used for writing log statements
    private static final Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public static Boolean getServer() {
        return true;
    }

    // username of MySQL
    public static String getDBUser() {
        return "root";
    }

    // password of MySQL
    public static String getDBPass() {
        return "password";
    }

    // return connection url for MySQL
    public static String getConPath() {
        if (getServer())
            return "jdbc:mysql://localhost:3306/";
        else
            return "null";
    }

    // check is the MySQL driver is available
    public static Connection getConnection() throws SQLException {
        if (getServer())
            return DriverManager.getConnection(getConPath(), getDBUser(), getDBPass());
        else
            return DriverManager.getConnection(getConPath());
    }

    // get name of the database as 'tpch'
    public static String getTablePrefix() {
        if (getServer())
            return "tpch.";
        else
            return "";
    }

    // to read the configuration file for each server or client
    public static Properties readPropertiesFile(String fileName) {
        FileInputStream fileInputStream;
        Properties properties = null;
        try {
            // associating file input stream to properties file
            fileInputStream = new FileInputStream(fileName);
            properties = new Properties();
            // loading properties file
            properties.load(fileInputStream);
        } catch (IOException ioException) {
            log.log(Level.SEVERE, ioException.getMessage());
        }
        return properties;
    }

    // perform modulus operation on 'int'
    public static int mod(int number) {
        int modulo = 100000007;
        number = number % modulo;
        // in case mod of the number is negative
        if (number < 0)
            number = number + modulo;
        return number;
    }

    // perform modulus operation on 'long'
    public static long mod(long number) {
        long modulo = 100000007;
        number = number % modulo;
        // in case mod of the number is negative
        if (number < 0)
            number = number + modulo;
        return number;
    }

    // convert a string into array of numbers
    public static int[] stringToIntArray(String data) {
        int[] result = new int[data.length()];
        for (int i = 0; i < data.length(); i++) {
            result[i] = (data.charAt(i) - '0');
        }
        return result;
    }

    // to print the result in a file for list datatype
    public static void printResult(List<Integer> result, String fileName) throws IOException {
        System.out.println("The number of rows matching the query is " + result.size());

        String pathName = "result/";
        // creating result directory if not present
        Files.createDirectories(Paths.get(pathName));
        FileWriter writer = new FileWriter(pathName + fileName);

        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        // writing data to file
        for (int data:result) {
            bufferedWriter.append(String.valueOf(data)).append(",");
        }
        bufferedWriter.close();
    }

    // to print the result in a file for set datatype
    public static void printResult(Set<Integer> result, String fileName) throws IOException {
        System.out.println("The number of rows matching the query is " + result.size());

        String pathName = "result/";
        // creating result directory if not present
        Files.createDirectories(Paths.get(pathName));
        FileWriter writer = new FileWriter(pathName + fileName);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        // writing data to file
        for (int data:result) {
            bufferedWriter.append(String.valueOf(data)).append(",");
        }
        bufferedWriter.close();
    }

    // to print the result in a file for int array datatype
    public static void printResult(int[][] result, int[] query, String fileName) throws IOException {


        String pathName = "result/";
        // creating result directory if not present
        Files.createDirectories(Paths.get(pathName));
        FileWriter writer = new FileWriter(pathName + fileName);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        // writing data to file
        for (int i = 0; i < result.length; i++) {
            bufferedWriter.append(String.valueOf(query[i] + 1)).append("\n");
            for (int j = 0; j < result[0].length; j++) {
                bufferedWriter.append(String.valueOf(result[i][j])).append(",");
            }
            bufferedWriter.append("\n");
        }
        bufferedWriter.close();
    }

    // computing time taken for each program
    public static ArrayList<Long> getProgramTimes(ArrayList<Instant> timestamps) {

        ArrayList<Long> durations = new ArrayList<>();

        for (int i = 0; i < timestamps.size() - 1; i++) {
            durations.add(Duration.between(timestamps.get(i), timestamps.get(i + 1)).toMillis());
        }

        return durations;
    }

    // converting a string array to a string
    public static String strArrToStr(String[] arr) {
        ArrayList<String> arrAsList = new ArrayList<>(Arrays.asList(arr));
        return arrAsList.stream().map(Object::toString).collect(Collectors.joining(", "));
    }

    // converting an integer 1D array to a string
    public static String arrToStr(int[] arr) {
        ArrayList<Integer> arrAsList = new ArrayList<>();
        for (Integer num : arr)
            arrAsList.add(num);
        return arrAsList.stream().map(Object::toString).collect(Collectors.joining(", "));
    }

    // converting an integer 2D array to a string
    public static String arrToStr(int[][] arr) {
        String str = Arrays.deepToString(arr);
        str = str.replaceAll("\\], \\[", "\n");
        str = str.replaceAll("\\], \\[", "");
        str = str.replaceAll("\\[\\[", "");
        str = str.replaceAll("\\]\\]", "");

        return str;
    }

    // converting a string to a string array
    public static int[][] strToStrArr1(String data) {
        String[] temp = data.split("\n");
        int[][] result = new int[temp.length][];

        int count = 0;
        for (String line : temp) {
            result[count++] = Stream.of(line.split(", "))
                    .mapToInt(Integer::parseInt)
                    .toArray();
        }
        return result;
    }

    // converting a string to a string array
    public static int[] strToArr(String str) {
        ArrayList<Integer> arrList = new ArrayList<>();
        String temp[];

        if (str.contains(", "))
            temp = str.split(", ");

        else if (str.contains("|"))
            temp = str.split("\\|");

        else
            temp = new String[]{str};


        for (String val : temp) {
            arrList.add(Integer.parseInt(val));
        }
        int[] result = new int[arrList.size()];

        for (int i = 0; i < result.length; i++) {
            result[i] = arrList.get(i);
        }

        return result;
    }

    // convert milliseconds to hour, minute and seconds
    public static String convertMillisecondsToHourMinuteAndSeconds(long milliseconds) {
        long seconds = (milliseconds / 1000) % 60;
        long minutes = (milliseconds / (1000 * 60)) % 60;
        long hours = (milliseconds / (1000 * 60 * 60)) % 24;
        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }

    // to help in showing progress bar
    public static void progressBar(double percentInp, long timeSinceStart) {
        int percent = (int) (percentInp + 0.5);
        if (percent == 0)
            percent = 1;

        String bar = "|";
        String progress = "";
        for (int i = 0; i < percent / 2 - 1; i++)
            progress += "=";
        if (percent == 100)
            progress += "=";
        else
            progress += ">";
        for (int i = 0; i < 50 - percent / 2; i++)
            progress += "-";
        String finalString = bar + progress + bar + " " + percent + "%  |  Est. Time Remaining: " + convertMillisecondsToHourMinuteAndSeconds(timeSinceStart * (100 - percent) / percent) + "        ";
        if (percent != 100)
            finalString += " \r";
        else
            finalString += " \n";
        System.out.print(finalString);
    }


    // convert long array to string
    public static String arrToStr(long[] arr) {
        ArrayList<Long> arrAsList = new ArrayList<>();
        for (Long num : arr)
            arrAsList.add(num);
        return arrAsList.stream().map(Object::toString).collect(Collectors.joining(", "));
    }


    // convert 2D string array to string
    public static String arrToStr(String[][] arr) {
        String str = Arrays.deepToString(arr);
        str = str.replaceAll("\\], \\[", "\n");
        str = str.replaceAll("\\], \\[", "");
        str = str.replaceAll("\\[\\[", "");
        str = str.replaceAll("\\]\\]", "");

        return str;
    }

    // convert long 2D array to string
    public static String arrToStr(long[][] arr) {
        String str = Arrays.deepToString(arr);
        str = str.replaceAll("\\], \\[", "\n");
        str = str.replaceAll("\\], \\[", "");
        str = str.replaceAll("\\[\\[", "");
        str = str.replaceAll("\\]\\]", "");

        return str;
    }

    // convert list to string
    public static <T> String listToStr(ArrayList<T> list) {
        return list.stream().map(Object::toString).collect(Collectors.joining(", "));
    }

    // convert string to long array
    public static long[] strToArr1(String str) {
        ArrayList<Long> arrList = new ArrayList<>();
        String temp[];

        if (str.contains(", "))
            temp = str.split(", ");

        else if (str.contains("|"))
            temp = str.split("\\|");

        else
            temp = new String[]{str};


        for (String val : temp) {
            arrList.add(Long.parseLong(val));
        }
        long[] result = new long[arrList.size()];

        for (int i = 0; i < result.length; i++) {
            result[i] = arrList.get(i);
        }

        return result;
    }

    // convert list to integer array
    public static int[][] strToArr(ArrayList<String> list, int startRow, int endRow) {
        int numValsInRow = Helper.strToArr(list.get(startRow)).length;

        int[][] result = new int[endRow - startRow][numValsInRow];

        for (int i = startRow; i < endRow; i++) {
            int[] arr = Helper.strToArr(list.get(i));
            System.arraycopy(arr, 0, result[i - startRow], 0, numValsInRow);
        }

        return result;
    }

    // convert list to long array
    public static long[][] strToArr1(ArrayList<String> list, int startRow, int endRow) {
        int numValsInRow = Helper.strToArr1(list.get(startRow)).length;

        long[][] result = new long[endRow - startRow][numValsInRow];

        for (int i = startRow; i < endRow; i++) {
            long[] arr = Helper.strToArr1(list.get(i));
            System.arraycopy(arr, 0, result[i - startRow], 0, numValsInRow);
        }

        return result;
    }

    // convert string to string array
    public static String[] strToStrArr(String str) {
        String result[] = str.split(", ");
        return result;
    }
}