package in.gaurav.part03intermediate;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Intermediate04FullOuterJoin {
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

        final JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
        final JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

        // Full outer join: All data in both RDDs will be present. So, there is a potential for "empty" on both sides
        final JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> joinedPairRDD = visits.fullOuterJoin(users);

        joinedPairRDD.foreach(pairRDD ->
                System.out.printf("Id: %s, Name: %s, Num Visits: %s%n",
                        pairRDD._1,
                        pairRDD._2._2.orElse("Name Unavailable"),
                        pairRDD._2._1.orElse(0))
        );

        sc.close();
    }
}
