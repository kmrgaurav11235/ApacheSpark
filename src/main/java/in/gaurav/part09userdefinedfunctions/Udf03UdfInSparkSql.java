package in.gaurav.part09userdefinedfunctions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Udf03UdfInSparkSql {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/newbiglog.txt");

        // Task: Create UDF to make this ugly Spark SQL query better.

        SimpleDateFormat inputDateFormat = new SimpleDateFormat("MMMM");
        SimpleDateFormat outputDateFormat = new SimpleDateFormat("M");

        spark.udf()
                .register("monthNum", (String month) -> {
                    final Date parsedDate = inputDateFormat.parse(month);
                    return Integer.parseInt(outputDateFormat.format(parsedDate));
        }, DataTypes.IntegerType);

        dataset.createOrReplaceTempView("logging_view");

        /*
        final Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, " +
                "count(1) as total from logging_view group by level, month " +
                "order by cast(first(date_format(datetime, 'M')) as int), level");
         */
        final Dataset<Row> results =
                spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total " +
                        "from logging_view " +
                        "group by level, month " +
                        "order by monthNum(month), level");

        results.show(100);

        spark.close();
    }
}
