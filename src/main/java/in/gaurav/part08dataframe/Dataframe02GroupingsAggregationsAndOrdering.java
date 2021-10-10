package in.gaurav.part08dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class Dataframe02GroupingsAggregationsAndOrdering {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/newbiglog.txt");

        /*
        final Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, " +
                "count(1) as total from logging_view group by level, month " +
                "order by cast(first(date_format(datetime, 'M')) as int), level");
         */

        dataset = dataset.select(
                col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("month_num").cast(DataTypes.IntegerType)
        );

        // There is also a version of groupBy() that uses String instead of Column
        // groupBy() method returns a "RelationalGroupedDataset" object. This class allows us to call the aggregation functions.
        dataset = dataset.groupBy(col("level"), col("month"), col("month_num")) // Returns "RelationalGroupedDataset"
                .count();// Returns Dataset

        dataset = dataset.orderBy("month_num", "level"); // There is also a version of orderBy() that uses Column instead of String

        dataset = dataset.drop("month_num"); // // There is also a version of drop() that uses Column instead of String

        dataset.show(100);

        spark.close();
    }
}
