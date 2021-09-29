package in.gaurav.part05performance;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Scanner;

public class Perf05CachingAndPersistence {

	public static void main(String[] args)
	{
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/bigLog.txt");

		System.out.println("Initial RDD Partition Size: " + initialRdd.getNumPartitions());

		// Narrow transformation
		JavaPairRDD<String, String> warningsAgainstDate = initialRdd.mapToPair(inputLine -> {
			String[] cols = inputLine.split(":");
			String level = cols[0];
			String date = cols[1];
			return new Tuple2<>(level, date);
		});
		
		System.out.println("After a narrow transformation we have " + warningsAgainstDate.getNumPartitions() + " parts");
		
		// Now we're going to do a Wide transformation
		JavaPairRDD<String, Iterable<String>> results = warningsAgainstDate.groupByKey();

		// If we are doing multiple Actions, we can cache or persist the results before we do any Action.
		// results = results.cache();
		// results = results.persist(StorageLevel.MEMORY_AND_DISK());

		System.out.println(results.getNumPartitions() + " partitions after the wide transformation");

		// After this action has been executed, once the program outputs the result, the real RDD in memory -- the
		// actual data -- will be discarded.
		results.foreach(it -> System.out.println("key " + it._1 + " has " + Iterables.size(it._2) + " elements"));

		/*
		This is a serious performance blunder. We would be able to see a new "count" job in http://localhost:4040.
		This new stage will do groupByKey again. Since the RDD data was discarded after line 48 was executed, to run
		this new Action, the program has to start again from start.
		There is performance optimization though -- whenever a shuffle happens, the data is written in the disk.
		So, during the shuffle in foreach-job, data was written to disk. the count-job restarts from this point.
		*/
		System.out.println(results.count());

		// This part will wait for user input so that we can view the Spark Jobs UI at http://localhost:4040
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();

		sc.close();
	}

}
