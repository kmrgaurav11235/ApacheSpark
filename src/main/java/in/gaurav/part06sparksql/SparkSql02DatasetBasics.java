package in.gaurav.part06sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql02DatasetBasics {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                // .config("spark.sql.warehouse.dir", "file:///c:/tmp/") // Only for windows -- directory to store temporary files
                .getOrCreate();

        final Dataset<Row> dataset = spark.read()
                .option("header", true) // Informs Spark that the CSV file has a header
                .csv("src/main/resources/exams/students.csv");

        final Row firstRow = dataset.first();
        // final String subject = firstRow.get(2).toString(); // get(index) returns "Object"
        final String subject = firstRow.getAs("subject"); // using the header row, instead of index. Also, getAs() tries to do conversion to datatype.
        System.out.println("Subject: " + subject);

        final int year = Integer.parseInt(firstRow.getAs("year"));
        System.out.println("Year: " + year);

        spark.close();
    }
}
