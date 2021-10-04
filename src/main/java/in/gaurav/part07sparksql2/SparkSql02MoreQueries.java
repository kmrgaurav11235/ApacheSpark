package in.gaurav.part07sparksql2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql02MoreQueries {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                .getOrCreate();

        final Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        dataset.createOrReplaceTempView("my_students_view");

        // final Dataset<Row> results = spark.sql("select max(score), min(score), avg(score) from my_students_view where subject='French'");
        final Dataset<Row> results = spark.sql("select distinct(year) from my_students_view order by year desc");

        results.show();

        spark.close();
    }
}
