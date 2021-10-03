package in.gaurav.part06sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql05FiltersUsingColumns {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                .getOrCreate();

        final Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        // Task: Filter the data -> only include the subject "Modern Art" and after year 2006

        // Using Columns: Use Column to build a condition.
        Column subjectColumn = dataset.col("subject");
        Column yearColumn = dataset.col("year");

        final Dataset<Row> modernArtResults = dataset.filter(
                subjectColumn
                        .equalTo("Modern Art")
                        .and(yearColumn.geq(2007))
        );

        modernArtResults.show();

        spark.close();
    }
}
