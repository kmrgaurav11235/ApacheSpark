package in.gaurav.part09userdefinedfunctions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class Udf02WritingUdf {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .getOrCreate();

        // If the data for the new column has a complex logic -- we use User Defined Functions (UDF)
        spark.udf()
                .register("hasPassed", (String grade, String subject) -> {
                    if ("Biology".equals(subject)) {
                        if (grade.startsWith("A") || grade.startsWith("B")) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                    return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
                }, DataTypes.BooleanType);

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        dataset = dataset.withColumn("pass",
                callUDF("hasPassed", col("grade"), col("subject")));

        dataset.show();

        spark.close();
    }
}
