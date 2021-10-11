package in.gaurav.part08dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class Dataframe03PivotTables {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/newbiglog.txt");

        dataset = dataset.select(
                col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("month_num").cast(DataTypes.IntegerType)
        );

        // pivot() is used to pivot/rotate the data from one DataFrame/Dataset column into multiple columns (transform
        // row to column).

        // This is the expected values for columns. You can choose not to provide this data. But, providing this leads
        // to improved performance.
        List<Object> months = List.of("January", "February", "March", "April", "May", "June", "July", "August",
                "September", "October", "November", "December");

        dataset = dataset.groupBy("level") // This will be the "Row" in the Pivot table.
                .pivot("month", months) // This will be the "Column" in the Pivot table.
                .count() // The aggregation operation to get the "Facts" inside the Pivot table.
                .na().fill(0); // This tells Spark to fill Facts as "0", if it cannot find data for a particular cell.

        dataset.show(100);

        spark.close();
    }
}
