package in.gaurav.part07sparksql2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class SparkSql06MultipleGroupings {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                .getOrCreate();

        /*
        List<Row> inMemory = new ArrayList<>();
        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

        StructField[] fields = {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };

        StructType schema = new StructType(fields);

        final Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);
        */

        final Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/newbiglog.txt");

        dataset.createOrReplaceTempView("logging_view");

        // We need count(1) as at-least one column needs to have aggregation operation when using "group by".
        final Dataset<Row> results =
                spark.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total "
                        + "from logging_view group by level, month");

        results.show(100);

        results.createOrReplaceTempView("results_view");
        final Dataset<Row> totals = spark.sql("select sum(total) from results_view");
        totals.show();

        spark.close();
    }
}
