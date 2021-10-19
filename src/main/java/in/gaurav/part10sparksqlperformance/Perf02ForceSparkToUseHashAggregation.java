package in.gaurav.part10sparksqlperformance;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Perf02ForceSparkToUseHashAggregation {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                .getOrCreate();

        spark.conf().set("spark.sql.shuffle.partitions", "12");

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/newbiglog.txt");

        dataset.createOrReplaceTempView("logging_view");

        Dataset<Row> results =
                spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total, " +
                        "first( cast( date_format( datetime, 'M' ) as int ) ) as month_num " + // Cast this to int makes this mutable
                        "from logging_view " +
                        "group by level, month " +
                        "order by month_num, level");

        results = results.drop("month_num");

        results.show(100);

        results.explain();

        /*
        * The HashMap used in Hash Aggregation is not the Java HashMap. Rather, it is implemented in native memory.
        * It only works when the "values" in kvp (i.e. the non group-by rows) is composed of "Mutable" data-types.
        * For Spark, Int, Boolean, double etc. are "Mutable" as they are of fixed length.
        * Strings are not mutable as they do not have a fixed length.
        * Hash Aggregation has been automatically used in Java API version. This is because in that version, "month_num"
            is a part of "key" not "value". So, it doesn't matter that it is not mutable.
        */

        spark.close();
    }
}
