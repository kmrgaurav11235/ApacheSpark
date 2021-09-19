package in.gaurav.part02moreoperations;

import in.gaurav.common.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class MoreOperations03SortAndCoalesce {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        final SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");

        final JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        initialRdd
                .map(sentence -> sentence
                        .replaceAll("[^a-zA-Z\\s]", " ") // Replace all non-alphabets and non-spaces with empty string
                        .toLowerCase()
                        .trim()) // Trim all spaces
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
                .filter(StringUtils::isNotBlank)
                .filter(Util::isNotBoring)
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey((value1, value2) -> value1 + value2)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1)) // Switching the values around
                .sortByKey(false)
                // .coalesce(1) // Moves all the data to a single partition. The problem with coalesce is simple -- we bring all the data
                // to a single machine. If it is Big data, further operations on it will result in out of memory errors.
                // .foreach(data -> System.out.println(data)); // for-each without coalesce doesn't print sorted data. This is because
                // for-each is executed on each partition in parallel and the output is interweaved.
                // .take(10) // Correct way to do it 1. -- first take, then print. In fact, any such action before for-each can be used.
                .collect() // Correct way to do it 1. -- first collect(), then print.
                .forEach(System.out::println);
        /*
        Correct use of coalesce(numPartitions):
        After performing many transformations and actions on multi-terabyte, multi-partition RDD, we have nor reached a point where we have
        only a small amount of data. For the remaining transformations, there is no point in continuing across thousands of partitions -- any
        shuffles will be pointlessly expensive. Here, we can use coalesce().
        coalesce() is just a way of reducing partitions in such cases; it is never a way to get the right answer.

        collect():
        collect() is generally used when you are finished and you want to gather a small RDD onto the driver node. This can then be used, for
        example, for printing. Only call this if you are sure that the RDD will fit into RAM of a single JVM. If results are still big, we
        can write into a distributed data store (e.g. HDFS File).
         */

        sc.close();
    }
}
