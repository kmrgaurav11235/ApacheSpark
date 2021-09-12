package in.gaurav.part01basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Basics06FlatMaps {
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

        // FlatMaps: Unlike 'map()' which produces 1 output for 1 input, 'flatmap()' produces multiple outputs for 1
        // input. The returned values should be in form of an Iterator. Java Collections have an iterator() method.
        sc.parallelize(inputData)
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .collect()
                .forEach(System.out::println);

        sc.close();
    }
}
