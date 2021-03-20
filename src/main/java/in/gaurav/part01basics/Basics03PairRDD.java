package in.gaurav.part01basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Basics03PairRDD {
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        final SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> originalLogMessages = sc.parallelize(inputData);

        final JavaPairRDD<String, String> pairRDD = originalLogMessages.mapToPair(rawValue -> {
            final String[] columns = rawValue.split(":"); // Separating the Logging Level and the Date

            final String level = columns[0];
            final String date = columns[1];

            return new Tuple2<>(level, date);
        });

        sc.close();
    }
}
