package in.gaurav.part07sparksql2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql07Ordering {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                .getOrCreate();

        final Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/newbiglog.txt");

        dataset.createOrReplaceTempView("logging_view");

        // We are performing first(date_format(datetime, 'M')) as any column which is not a part of the "grouping"
        // (here it is level and month) must have an aggregation function performed on it.
        final Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, " +
                "count(1) as total from logging_view group by level, month " +
                "order by cast(first(date_format(datetime, 'M')) as int), level");

        // Another way
        /*Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, " +
                "cast(first(date_format(datetime, 'M')) as int) as month_num, count(1) as total " +
                "from logging_view group by level, month order by month_num, level");

        results = results.drop("month_num"); // No need to keep month_num in the result as we were using it only for sorting
         */

        results.show(100);

        spark.close();
    }
}
