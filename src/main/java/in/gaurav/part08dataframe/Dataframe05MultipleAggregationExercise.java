package in.gaurav.part08dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

public class Dataframe05MultipleAggregationExercise {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        /*
        Exercise:
        1. Build a pivot table showing each subject down the "left-hand side" and years across the top.
        2. For each Subject and Year, we want:
            a) The average exam score.
            b) The standard deviation of scores.
        (All to 2 decimal places)
         */

        spark.close();
    }
}
