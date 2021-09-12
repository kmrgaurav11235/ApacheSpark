package in.gaurav.part02intermediate;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Intermediate01ReadingFromDisk {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        final SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");

        final JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        initialRdd.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .collect()
                .forEach(System.out::println);

        sc.close();
    }
}
