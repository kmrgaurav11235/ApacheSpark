package in.gaurav.part08dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class Dataframe01SqlVsDataframe {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                .getOrCreate();

        // "Dataframe" is just the name given to Dataset of Rows, Dataset<Row>.
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/newbiglog.txt");

        // We don't need to createOrReplaceTempView(), if we are using Dataframe APIs

        /*
        final Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, " +
                "count(1) as total from logging_view group by level, month " +
                "order by cast(first(date_format(datetime, 'M')) as int), level");
         */

        // dataset = dataset.selectExpr("level", "date_format(datetime, 'MMMM') as month"); // selectExpr() allows SQL like syntax

        dataset = dataset.select(col("level"), date_format(col("datetime"), "MMMM").alias("month"));

        dataset.show(100);

        spark.close();
    }
}
