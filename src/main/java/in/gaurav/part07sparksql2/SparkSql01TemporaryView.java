package in.gaurav.part07sparksql2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql01TemporaryView {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                .getOrCreate();

        final Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        // View -> Enhanced version of Dataset
        // createOrReplaceTempView() doesn't actually return anything.
        dataset.createOrReplaceTempView("my_students_view"); // Creates a temporary view named "students" in memory

        // Now, we can refer to this view "students" in SQL statements
        final Dataset<Row> results = spark.sql("select score, year from my_students_view where subject='French'");

        results.show();

        spark.close();
    }
}
