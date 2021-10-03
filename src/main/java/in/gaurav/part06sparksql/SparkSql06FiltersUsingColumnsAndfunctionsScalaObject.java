package in.gaurav.part06sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SparkSql06FiltersUsingColumnsAndfunctionsScalaObject {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                .getOrCreate();

        final Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        // Task: Filter the data -> only include the subject "Modern Art" and after year 2006

        // Using Columns: with scala functions object, col() method
        final Dataset<Row> modernArtResults = dataset.filter(
                col("Modern Art") // Comes from the static import org.apache.spark.sql.functions.col
                        .equalTo("Modern Art")
                        .and(col("year").geq(2007))
        );

        modernArtResults.show();

        spark.close();
    }
}
