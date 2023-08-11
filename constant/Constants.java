package constant;

public class Constants {

    // minimum bound for generating random number
    private static final int minRandomBound = 1;
    // maximum bound for generating random number
    private static final int maxRandomBound = 10;
    // number of threads max in a pool
    private static final int threadPoolSize = 10;
    // maximum length of string considered
    private static final int numberSize = 10;

    public static int getMinRandomBound() {
        return minRandomBound;
    }

    public static int getMaxRandomBound() {
        return maxRandomBound;
    }

    public static int getThreadPoolSize() {
        return threadPoolSize;
    }

    public static int getNumberSize() {
        return numberSize;
    }

}