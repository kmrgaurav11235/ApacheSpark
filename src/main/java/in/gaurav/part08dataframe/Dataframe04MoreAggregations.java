package in.gaurav.part08dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

public class Dataframe04MoreAggregations {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                //.option("inferSchema", true) // Infers the input schema automatically from data. But, it does require one extra pass over data (Very expensive).
                .csv("src/main/resources/exams/students.csv");

        dataset = dataset.groupBy("subject")
                .agg(
                        max("score").alias("max_score"),
                        min("score").alias("min_score")
                );

        dataset.show();

        spark.close();
    }
}
