package in.gaurav.part02intermediate;

import in.gaurav.common.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/*
1. Upload the input.txt file to S3 and update its location on line 30.
2. Upload the jar created from this project to s3.
3. Deploy an Elastic Map-Reduce Spark Cluster.
4. Add entries to its inbound firewall rules on ports 22(ssh) and 18080 (History Server).
5. ssh into it `ssh -i key.pem hadoop@dnsname_of_master`
6. Use `aws s3 cp <url>` command to download the jar file.
7. Trigger the spark job: `spark-submit <jar_file>`.
8. Open this URL in your browser: `<dns>:18080` to open the "History Server" and view all the Spark applications that
    have completed.
*/

public class Intermediate04KeywordRankingInSparkCluster {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        final SparkConf conf = new SparkConf()
                .setAppName("startingSpark");
                // .setMaster("local[*]"); For local run

        final JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("s3n://test-kg-demo-619/input-spring.txt");

        System.out.println("Top 10 keywords for this course:");

        initialRdd
                .map(sentence -> sentence
                        .replaceAll("[^a-zA-Z\\s]", " ")
                        .toLowerCase()
                        .trim()) // Trim all spaces
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
                .filter(StringUtils::isNotBlank)
                .filter(Util::isNotBoring)
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey((value1, value2) -> value1 + value2)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false)
                .take(10)
                .forEach(System.out::println);

        sc.close();
    }
}
