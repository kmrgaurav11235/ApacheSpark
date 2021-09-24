package in.gaurav.part04bigdataexercise;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Exercise02MostPopularCourse {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Use true to use hardcoded data identical to that in the PDF guide.
        boolean testMode = false;

        JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
        JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
        JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

        /*
        Exercise 2: Your job is to produce a ranking chart detailing which are the most popular courses by score.
        Business Rules:
        We think that if a user sticks it through most of the course, that's more deserving of "points" than if
        someone bails out just a quarter way through the course. So we've cooked up the following scoring system:
        -> If a user watches more than 90% of the course, the course gets 10 points
        -> If a user watches > 50% but <90% , it scores 4
        -> If a user watches > 25% but < 50% it scores 2
        -> Less than 25% is no score
         */

        // Step 1: Remove duplicate views and flip the key and value of
        viewData = viewData.distinct();

        // Step 2: Join the chapterData with viewData
        viewData = viewData.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1)); //Now it has (chapterId, userId)
        // (chapterId, (userId, courseId))
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRDD = viewData.join(chapterData);

        // Step 3: Drop the chapterId. We now know that every row in the RDD is a distinct chapter in the course.
        // ((userId, courseId), 1L)
        JavaPairRDD<Tuple2<Integer, Integer>, Long> userCourseViewData =
                joinedRDD.mapToPair(tuple -> new Tuple2<>(tuple._2, 1L));

        // Step 4: Count views of (userId, courseId)
        // ((userId, courseId), numViews)
        userCourseViewData = userCourseViewData.reduceByKey((views1, views2) -> views1 + views2);

        // Step 5: Drop the userId
        // (courseId, numViews)
        final JavaPairRDD<Integer, Long> courseViewData =
                userCourseViewData.mapToPair(tuple -> new Tuple2<>(tuple._1._2, tuple._2));

        // Step 6: Add the total chapter count in a course
        // (courseId, numChapters)
        final JavaPairRDD<Integer, Integer> chapterCountData = chapterData
                .mapToPair(tuple -> new Tuple2<>(tuple._2, 1))
                .reduceByKey((val1, val2) -> val1 + val2);

        // (courseId, (numViews, numChapters))
        final JavaPairRDD<Integer, Tuple2<Long, Integer>> courseViewsAndChapterCountData =
                courseViewData.join(chapterCountData);

        // Step 7: Ratio of course watched
        // (courseId, ratioOfCourseWatched)
        final JavaPairRDD<Integer, Double> watchedCourseRatioData =
                courseViewsAndChapterCountData.mapValues(value -> (double) value._1 / value._2);
        // mapValues -> If you are not changing the keys, but only values you can use this.

        // Step 8: Convert ratios to scores based on the business rules provided
        // (courseId, score)
        final JavaPairRDD<Integer, Long> scoreData = watchedCourseRatioData.mapValues(value -> {
            if (value > 0.9) {
                return 10L;
            } else if (value > 0.5) {
                return 4L;
            } else if (value > 0.25) {
                return 2L;
            } else {
                return 0L;
            }
        });

        // Step 9: Get total score
        // (courseId, totalScore)
        final JavaPairRDD<Integer, Long> totalScoreData = scoreData.reduceByKey((score1, score2) -> score1 + score2);

        // Step 10: Add Course Title
        // (courseId, (totalScore, courseTitle))
        final JavaPairRDD<Integer, Tuple2<Long, String>> totalScoreWithCourseNameData = totalScoreData.join(titlesData);

        // Step 11: Remove courseId and sort by totalScore
        // (totalScore, courseTitle)
        final JavaPairRDD<Long, String> popularCourseData = totalScoreWithCourseNameData
                        .mapToPair(tuple -> new Tuple2<>(tuple._2._1, tuple._2._2))
                        .sortByKey(false);

        popularCourseData.collect().forEach(System.out::println);

        sc.close();
    }

    private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
        if (testMode) {
            // (chapterId, title)
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
            rawTitles.add(new Tuple2<>(1, "How to find a better job"));
            rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs(rawTitles);
        }
        return sc.textFile("src/main/resources/viewing figures/titles.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, String>(new Integer(cols[0]), cols[1]);
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
        if (testMode) {
            // (chapterId, (courseId, courseTitle))
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96, 1));
            rawChapterData.add(new Tuple2<>(97, 1));
            rawChapterData.add(new Tuple2<>(98, 1));
            rawChapterData.add(new Tuple2<>(99, 2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));
            return sc.parallelizePairs(rawChapterData);
        }

        return sc.textFile("src/main/resources/viewing figures/chapters.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
        if (testMode) {
            // Chapter views - (userId, chapterId)
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            return sc.parallelizePairs(rawViewData);
        }

        return sc.textFile("src/main/resources/viewing figures/views-*.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] columns = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
                });
    }
}
