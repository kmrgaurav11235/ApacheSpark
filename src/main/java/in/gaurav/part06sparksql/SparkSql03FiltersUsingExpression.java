package in.gaurav.part06sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql03FiltersUsingExpression {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql").master("local[*]")
                .getOrCreate();

        final Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        // Task: Filter the data -> only include the subject "Modern Art" and after year 2006

        // Using condition Expression: Filter rows using the "where" clause in SQL expression.
        // Strings inside the expression are in single quotes (')
        final Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' and year >= 2007");
        modernArtResults.show();

        spark.close();
    }
}
