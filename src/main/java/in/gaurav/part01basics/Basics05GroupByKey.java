package in.gaurav.part01basics;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Basics05GroupByKey {
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        final SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");

        final JavaSparkContext sc = new JavaSparkContext(conf);

        // IMPORTANT: groupByKey can be very problematic in production systems. So, use it carefully.

        sc.parallelize(inputData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .groupByKey() // Returns a tuple of key and an Iterable object with 2nd column grouped.
                .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances."));
        // Iterables.size() is guava -- it is Google's Java library with helpful functionalities.

        sc.close();
    }
}
