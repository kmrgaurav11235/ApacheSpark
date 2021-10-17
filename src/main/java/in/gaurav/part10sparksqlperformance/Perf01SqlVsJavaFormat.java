package in.gaurav.part10sparksqlperformance;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Scanner;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class Perf01SqlVsJavaFormat {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                .getOrCreate();

        // Configures the number of partitions that are used when shuffling data for joins or aggregations. Default = 200.
        spark.conf().set("spark.sql.shuffle.partitions", "12");

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/newbiglog.txt");

        /*dataset.createOrReplaceTempView("logging_view");

        final Dataset<Row> results =
                spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
                        "from logging_view " +
                        "group by level, month " +
                        "order by cast(first(date_format(datetime, 'M')) as int), level");


        results.show(100);*/

        dataset = dataset.select(
                col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("month_num").cast(DataTypes.IntegerType)
        );

        dataset = dataset.groupBy("level", "month", "month_num").count().alias("total");
        dataset = dataset.orderBy("month_num", "level");
        dataset = dataset.drop("month_num");
        dataset.show(100);

        // This part will wait for user input so that we can view the Spark Jobs UI at http://localhost:4040
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        spark.close();
    }
}
