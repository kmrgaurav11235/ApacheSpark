package in.gaurav.part01basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Basics01ReduceAndMap {
    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(3);
        inputData.add(12);
        inputData.add(31);
        inputData.add(45);
        inputData.add(12);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRDD = sc.parallelize(inputData);
        System.out.println("List: " + inputData);

        // Reduce
        final Integer result = myRDD.reduce((value1, value2) -> value1 + value2);
        System.out.println("\nSum: " + result);

        // Map
        final JavaRDD<Double> sqrtRDD = myRDD.map(value -> Math.sqrt(value));

        System.out.println("\nSquare Roots:");
        // Old Style
        // sqrtRDD.foreach(value -> System.out.println(value));

        // New Style. collect() method is there to prevent NotSerializableException.
        // This is because System.out.println() method is not serializable.
        sqrtRDD.collect().forEach(System.out::println);

        // Count the number of elements in sqrtRDD using just map and reduce: This is well known pattern
        final Long count = sqrtRDD.map(value -> 1L)
                .reduce((value1, value2) -> value1 + value2);
        System.out.println("\nCount of sqrtRDD:" + count);

        sc.close();
    }
}
