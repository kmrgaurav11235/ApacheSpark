package in.gaurav.part09userdefinedfunctions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class Udf01AddNewColumnToDataset {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        // Add a new column -- the data for the new column has a very simple logic
        dataset = dataset.withColumn("pass", lit(col("grade").equalTo("A+")));

        dataset.show();

        spark.close();
    }
}
