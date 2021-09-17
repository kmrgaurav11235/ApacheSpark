package in.gaurav.part03intermediate;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Intermediate01InnerJoins {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        final SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");

        final JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>(); // Tuple of userId and numVisits
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>(); // Tuple of userId and userName
        usersRaw.add(new Tuple2<>(1, "John McClane"));
        usersRaw.add(new Tuple2<>(2, "Hans Gruber"));
        usersRaw.add(new Tuple2<>(3, "Al Powell"));
        usersRaw.add(new Tuple2<>(4, "Harry Ellis"));
        usersRaw.add(new Tuple2<>(5, "Richard Thornburg"));
        usersRaw.add(new Tuple2<>(6, "Simon Gruber"));

        // Here we use Pair RRDs
        final JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
        final JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

        // Inner join: Discard any data that doesn't have a corresponding entry in the other RDD
        final JavaPairRDD<Integer, Tuple2<Integer, String>> joinedPairRDD = visits.join(users);

        joinedPairRDD.collect().forEach(System.out::println);

        sc.close();
    }
}
