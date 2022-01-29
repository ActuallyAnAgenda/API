import java.text.ParseException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Main {
    static class DynamicTask {
        final String ID;
        // The definition of duration is such that the sum of all partitioned work session intervals must have a length equal to this duration.
        final long duration;
        // The definition of the due date is such that the end time of all work session intervals [start, end) must be less than or equal to this due date.
        final long due;

        public DynamicTask(String ID, long due, long duration) {
            this.ID = ID;
            this.duration = duration;
            this.due = due;
        }
    }

    static class StaticTask {
        final String ID;
        // [start, start + duration)
        final long duration;
        final long start;

        public StaticTask(String ID, long start, long duration) {
            this.ID = ID;
            this.duration = duration;
            this.start = start;
        }
    }

    static class RecurringTask {
        public final static long MILLIS_IN_DAY = 1000 * 24 * 60 * 60;
        // Both start and end time are modulo (1000 * 24 * 60 * 60)
        // Like all events, the range is represented as [startTime, startTime + duration)
        private final long startTime;
        private final long duration;

        public RecurringTask(long startTime, long duration) {
            this.startTime = startTime % MILLIS_IN_DAY;
            this.duration = duration;
            if (duration >= MILLIS_IN_DAY || duration <= 0) {
                throw new IllegalArgumentException("Illegal duration.");
            }
        }
    }

    // Convex Hull Trick
    static class Parabola {
        // pow(x - h, 2) + k
        // For the case of this dp, the parabola represents the
        // cost incurred by adding onto the lowest cost suffix of the binary string,
        // with its leftmost index at 'idx'
        // Convex Hull Trick is utilized to exploit the fact that we are adding and querying parabolas in
        // order of their index / h value (since these are the same but offset)
        // in order to efficiently figure out which suffix to append a substring of either 0s or 1s
        // upto a new index, which is the 'y' value in the cost() function.
        private int h, k, idx;

        public Parabola(int a, int b, int c) {
            h = a;
            k = b;
            idx = c;
        }

        public int cost(long x) {
            return (int) Math.pow(x - h, 2) + k;
        }

        public int idx() {
            return idx;
        }

        public int intersect(Parabola comp) {
            return ((comp.k - k + (comp.h * comp.h) - (h * h)) / (2 * (comp.h - h)));
        }
    }

    // Assumes that there will be no two parabolas with the same horizontal shift.
    // Allows for the addition and query of parabolas in the form (x-h)^2 + k in sorted order of h, either left or right.
    // Note that the order (left to right or right to left) for queries and additions must be identical.
    static class MinPointParabolaQuery {
        private Parabola[] hull;
        private final boolean reverse; // Default order is left to right
        private int ls, rs;

        public MinPointParabolaQuery(int maxSizeBuffer, boolean reverse) {
            hull = new Parabola[maxSizeBuffer];
            this.reverse = reverse;
        }

        // Inserts a parabola to the convex hull.
        public void insert(Parabola par) {
            while (rs - ls > 1 && (hull[rs - 2].intersect(hull[rs - 1]) > hull[rs - 2].intersect(par)) ^ reverse) rs--;
            hull[rs++] = par;
        }

        // Queries for the parabola with the smallest y value at x.
        public Parabola query(int x) {
            while (rs - ls > 1 && hull[ls].cost(x) >= hull[ls + 1].cost(x)) ls++;
            return hull[ls];
        }
    }

    public static long epochFromDate(String d) throws ParseException {
        return (ZonedDateTime.parse(
                d ,
                DateTimeFormatter.ofPattern ( "yyyy MM dd HH mm zzz" )
        )
                .toInstant()
                .toEpochMilli());
    }

    public static long epochFromMinutes(int cnt) {
        return (long) cnt * 1000 * 60;
    }

    public static void main(String[] args) throws ParseException {
        List<DynamicTask> todo = new ArrayList<>();
        List<StaticTask> events = new ArrayList<>();
        List<RecurringTask> preferences = new ArrayList<>();

        preferences.add(new RecurringTask(epochFromDate("2008 01 06 00 00 EST"), epochFromMinutes(60 * 8))); // 12 - 8 sleep
        preferences.add(new RecurringTask(epochFromDate("2008 01 06 09 00 EST"), epochFromMinutes(15))); // breakfast at 9am
        preferences.add(new RecurringTask(epochFromDate("2008 01 06 13 30 EST"), epochFromMinutes(15))); // lunch at 1:30
        preferences.add(new RecurringTask(epochFromDate("2008 01 06 19 30 EST"), epochFromMinutes(15))); // dinner at 7

        todo.add(new DynamicTask("Test Project 1", epochFromDate("2022 08 10 00 00 EST"), epochFromMinutes(240)));
        todo.add(new DynamicTask("ActuallyAnAgenda", epochFromDate("2022 08 15 00 00 EST"), epochFromMinutes(600)));
        int optimalK = 2; // 30 minutes
        //System.out.println(calcMinCost("00110110011", 2));
        Result<List<StaticTask>> tasks = partitionDynamicTasks(optimalK, todo, events, preferences);
        if(tasks.isSuccess()) {
            for (StaticTask st : tasks.get()) {
                System.out.println(new Date(st.start) + " to " + new Date(st.duration + st.start));
            }
        } else {
            System.out.println(tasks.getMessage());
        }

    }

    public static boolean[] binaryStringToBoolArray(String S) {
        boolean[] ret = new boolean[S.length()];
        for (int i = 0; i < S.length(); i++) {
            ret[i] = S.charAt(i) == '1';
        }
        return ret;
    }

    public static String boolArrayToBinaryString(boolean[] b) {
        StringBuilder sb = new StringBuilder();
        for (boolean bool : b) {
            sb.append(bool ? '1' : '0');
        }
        return sb.toString();
    }

    public static String calcMinCost(String S, int K) {
        //System.out.println(S);
        int[] binaryArr = new int[S.length()];
        for (int i = 0; i < S.length(); i++) binaryArr[i] = S.charAt(i) - '0';
        int[] pref = new int[S.length() + 1];

        pref[S.length() - 1] = binaryArr[S.length() - 1];
        for (int i = S.length() - 2; i >= 0; i--) {
            pref[i] = pref[i + 1] + binaryArr[i];
        }

        int N = S.length();
        int[][][] dp = new int[S.length()][pref[0] + 1][2];
        int[][][] prev = new int[S.length()][pref[0] + 1][2];
        // zeroSuffixes[k] stores dp[i][j][0] such that i + j = k
        MinPointParabolaQuery[] zeroSuffixes = new MinPointParabolaQuery[N + 1];

        for (int i = 0; i <= N; i++) {
            zeroSuffixes[i] = new MinPointParabolaQuery(pref[0] + 1, true);
        }

        zeroSuffixes[N].insert(new Parabola(N - K, 0, N));
        // initializing dp[i][0][0]
        for (int i = N - 1; i >= 0; i--) {
            dp[i][0][0] = (N - i - K) * (N - i - K);
            prev[i][0][0] = N;
            zeroSuffixes[i].insert(new Parabola(i - K, dp[i][0][0], i));
        }

        for (int oneCount = 1; oneCount <= pref[0]; oneCount++) {
            // Calc 1 and then 0 after 1
            for (int i = N - 1; i >= 0; i--) {
                if (pref[i] < oneCount) continue;
                Parabola par = zeroSuffixes[i + oneCount].query(i);
                dp[i][oneCount][1] = par.cost(i);
                prev[i][oneCount][1] = par.idx();
            }

            MinPointParabolaQuery oneSuffixes = new MinPointParabolaQuery(N, true);

            for (int i = N - 2; i >= 0; i--) {
                if (pref[i + 1] < oneCount) continue;
                // This is the suffix that corresponds to everything to the right of 'i' (The latest suffix that can be appended to)
                int leftmostIndex = i + 1;
                oneSuffixes.insert(new Parabola(leftmostIndex - K, dp[leftmostIndex][oneCount][1], leftmostIndex));

                Parabola par = oneSuffixes.query(i);
                dp[i][oneCount][0] = par.cost(i);
                prev[i][oneCount][0] = par.idx();

                // Inserting this suffix ending with a 0 to its diagonal correspondant within the dp array
                zeroSuffixes[i + oneCount].insert(new Parabola(i - K, dp[i][oneCount][0], i));
            }
        }

        //printDPArray(dp);
        StringBuilder sb = new StringBuilder();
        int curI = 0, curJ = pref[0], curK = dp[0][curJ][0] < dp[0][curJ][1] ? 0 : 1;

        while (curI != N) {
            int next = prev[curI][curJ][curK];
            int dist = next - curI; // length of current subsequence
            sb.append(String.valueOf(curK).repeat(dist));
            curI = next;
            if (curK == 1) curJ -= dist;
            curK ^= 1;
        }
        return sb.toString();
    }




    final static long CONVERSION_FACTOR = 15 * 1000 * 60;

    public static long epochToIntervalTime(long epoch) {
        return epoch / CONVERSION_FACTOR;
    }

    public static long intervalToEpochTime(long interval) {
        return interval * CONVERSION_FACTOR;
    }

    public static int convertAbsoluteToScheduleTime(long interval, long scheduleStart) {
        return (int) (interval - scheduleStart);
    }

    public static class Result<T> {
        private final T res;
        private final boolean success;
        private final String message;

        public Result(T res, boolean success, String message) {
            this.res = res;
            this.success = success;
            this.message = message;
        }

        public T get() {
            return res;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getMessage() {
            return message;
        }
    }

    /**
     * Partitions Dynamic Tasks into Static Ones, returned in Sorted Order.
     */
    public static Result<List<StaticTask>> partitionDynamicTasks(int optimalK, List<DynamicTask> dynamicTasks, List<StaticTask> staticTasks, List<RecurringTask> recurringTasks) {
        if (dynamicTasks.size() == 0) return new Result<>(new ArrayList<>(), true, "Successfully Partitioned 0 tasks.");

        // ------------------------------ READ ME ----------------------------------
        // To make the method more organized, ABSOLUTE TIMES will be stored in longs (15 minute intervals since Unix),
        // and RELATIVE TIMES will be stored in ints (15 minute intervals since the HARD_START_TIME)
        //
        // EPOCH TIMES will never be used aside from storage as tasks.
        // ------------------------------ READ ME ----------------------------------

        Collections.sort(dynamicTasks, Comparator.comparingLong(task -> task.due));

        // All of the below variables are in Interval Time.
        final long REST_FROM_CREATION = 2; // The amount of 15 minute-intervals from now until the scheduler is allowed to partition tasks.
        final long HARD_START_TIME = epochToIntervalTime(System.currentTimeMillis()) + REST_FROM_CREATION;
        //System.out.println(HARD_START_TIME);
        final long HARD_END_TIME = epochToIntervalTime(dynamicTasks.get(dynamicTasks.size() - 1).due);
        //System.out.println(HARD_END_TIME);

        if (HARD_END_TIME < HARD_START_TIME)
            return new Result<>(null, false, "All of your due-dates are prior or too close to the current time!");

        // isAvailable[relativeInterval] checks if the given relative interval
        boolean[] isAvailable = new boolean[convertAbsoluteToScheduleTime(HARD_END_TIME, HARD_START_TIME)];
        Arrays.fill(isAvailable, true);
        // inRecurringInterval[absoluteInterval] checks if that given absolute interval is within a recurring event.
        boolean[] inRecurringInterval = new boolean[(int) (RecurringTask.MILLIS_IN_DAY / CONVERSION_FACTOR)];

        // Loops through the recurring events and updates inRecurringInterval, which is a
        // universal array that checks for if any given interval is unavailable for partitioning due to recurring events.
        for (int i = 0; i < recurringTasks.size(); i++) {
            long start = epochToIntervalTime(recurringTasks.get(i).startTime);
            long dur = epochToIntervalTime(recurringTasks.get(i).duration);
            for (long j = start; j < start + dur; j++) {
                // If the event "overflows" to the next day, must use modulo
                inRecurringInterval[(int) j % inRecurringInterval.length] = true;
            }
        }
        for (long i = HARD_START_TIME; i < HARD_END_TIME; i++) {
            if (inRecurringInterval[(int) i % inRecurringInterval.length]) {
                isAvailable[convertAbsoluteToScheduleTime(i, HARD_START_TIME)] = false;
            }
        }

        for (int i = 0; i < staticTasks.size(); i++) {
            long start = epochToIntervalTime(staticTasks.get(i).start);
            long dur = epochToIntervalTime(staticTasks.get(i).duration);
            for (long j = Math.max(HARD_START_TIME, start); j < Math.min(HARD_END_TIME, start + dur); j++) {
                isAvailable[convertAbsoluteToScheduleTime(j, HARD_START_TIME)] = false;
            }
        }

        // Creating a separate partitioning array with the unavailable intervals removed
        // Firstly, index conversion arrays must be created to go back and forth
        int[] partitionIndex = new int[isAvailable.length];
        int cnt = 0;
        for (int i = 0; i < isAvailable.length; i++) {
            if (isAvailable[i]) {
                partitionIndex[i] = cnt++;
            }
        }
        int[] originalIndex = new int[cnt];
        for (int i = 0; i < isAvailable.length; i++) {
            if (isAvailable[i]) {
                originalIndex[partitionIndex[i]] = i;
            }
        }
        boolean[] naivePartition = new boolean[cnt];
        // Next, Attempting to create the naive partition generation -> Shoving all tasks as far back as possible
        int currentTaskIdx = dynamicTasks.size() - 1;
        long sessionsRemaining = epochToIntervalTime(dynamicTasks.get(currentTaskIdx).duration);
        for (long i = HARD_END_TIME - 1; i >= HARD_START_TIME; i--) {
            if (sessionsRemaining == 0) {
                currentTaskIdx--;
                if (currentTaskIdx < 0) break;
                sessionsRemaining = epochToIntervalTime(dynamicTasks.get(currentTaskIdx).duration);
            }
            if (!isAvailable[convertAbsoluteToScheduleTime(i, HARD_START_TIME)] || i >= epochToIntervalTime(dynamicTasks.get(currentTaskIdx).due))
                continue;
            naivePartition[partitionIndex[convertAbsoluteToScheduleTime(i, HARD_START_TIME)]] = true;
            sessionsRemaining--;
        }
        if (sessionsRemaining == 0) currentTaskIdx--; // Barely finished partitioning - the schedule is extremely full
        if (currentTaskIdx >= 0) {
            // Still tasks left to partition but with no room
            return new Result<>(null, false, "Error: It is impossible to generate a schedule without having overdue or overlapping tasks! Please modify the tasks, events, or preferences which you have set");
        }

        boolean[] fixedPartition = binaryStringToBoolArray(
                calcMinCost(
                        boolArrayToBinaryString(naivePartition),
                        optimalK)
        );

        Stack<StaticTask> result = new Stack<>();

        // Assigning optimal intervals to tasks using the previous method but in reverse
        currentTaskIdx = 0;
        sessionsRemaining = epochToIntervalTime(dynamicTasks.get(currentTaskIdx).duration);

        for (int i = 0; i < fixedPartition.length; i++) {
            if (!fixedPartition[i]) continue;
            if (sessionsRemaining == 0) {
                currentTaskIdx++;
                if (currentTaskIdx == dynamicTasks.size()) break;
                sessionsRemaining = epochToIntervalTime(dynamicTasks.get(currentTaskIdx).duration);
            }
            long trueTime = intervalToEpochTime(originalIndex[i] + HARD_START_TIME);
            String ID = dynamicTasks.get(currentTaskIdx).ID;
            if (result.isEmpty()) {
                // 1 interval
                result.push(new StaticTask(ID, trueTime, CONVERSION_FACTOR));
            } else {
                StaticTask top = result.peek();
                if (top.start + top.duration == trueTime) {
                    result.pop();
                    result.push(new StaticTask(ID, top.start, top.duration + CONVERSION_FACTOR));
                } else {
                    result.push(new StaticTask(ID, trueTime, CONVERSION_FACTOR));
                }
            }
            sessionsRemaining--;
        }

        return new Result<>(result, true, "Successfully Partitioned " + dynamicTasks.size() + " tasks.");
    }
}