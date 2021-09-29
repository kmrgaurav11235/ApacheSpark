package in.gaurav.part06sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql01GettingStarted {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                // .config("spark.sql.warehouse.dir", "file:///c:/tmp/") // Only for windows -- directory to store temporary files
                .getOrCreate();

        final Dataset<Row> dataset = spark.read()
                .option("header", true) // Informs Spark that the CSV file has a header
                .csv("src/main/resources/exams/students.csv");

        dataset.show(); // shows top 20 rows in a nice readable format.

        final long numberOfRows = dataset.count(); // Number of rows in the Dataset.
        System.out.println("There are " + numberOfRows + " records in the Dataset.");

        spark.close();
    }
}
