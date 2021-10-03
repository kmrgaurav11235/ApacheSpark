package in.gaurav.part06sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql04FiltersUsingLambdas {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                .getOrCreate();

        final Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        // Task: Filter the data -> only include the subject "Modern Art" and after year 2006

        // Using Lambdas: Each element is of type Row
        final Dataset<Row> modernArtResults = dataset.filter((Row row) ->
                row.getAs("subject").equals("Modern Art")
                        && Integer.parseInt(row.getAs("year")) >= 2007);
        modernArtResults.show();

        spark.close();
    }
}
