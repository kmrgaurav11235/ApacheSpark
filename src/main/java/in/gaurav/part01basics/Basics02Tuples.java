package in.gaurav.part01basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple5;

import java.util.ArrayList;
import java.util.List;

public class Basics02Tuples {
    public static void main(String[] args) {

        List<Integer> inputData = new ArrayList<>();
        inputData.add(3);
        inputData.add(12);
        inputData.add(31);
        inputData.add(45);
        inputData.add(12);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        final SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);

        // Tuples are scala concept. They are a small collection of items that we don't intend on modifying.
        final JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalIntegers.map(value -> new Tuple2<>(value, Math.sqrt(value)));
        sqrtRdd.collect().forEach(System.out::println);

        // There are other types of Tuples as well. You can go as far as Tuple22
        Tuple5<String, String, Integer, Double, List<String>> record
                = new Tuple5<>("Gaurav", "Kumar", 1, 91.5, List.of("Physics", "Chemistry", "Maths"));

        sc.close();
    }
}
