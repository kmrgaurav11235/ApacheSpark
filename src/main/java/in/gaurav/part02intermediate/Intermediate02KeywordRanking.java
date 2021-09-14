package in.gaurav.part02intermediate;

import in.gaurav.common.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/*
Input Subtitle File -> src/main/resources/subtitles/input.txt
Exclude words -> src/main/resources/subtitles/boringwords.txt
Make a list of most frequent words from the subtitle file
*/

public class Intermediate02KeywordRanking {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        final SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");

        final JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input-spring.txt");

        System.out.println("Top 10 keywords for this course:");

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
                .take(10)
                .forEach(System.out::println);

        sc.close();
    }
}
