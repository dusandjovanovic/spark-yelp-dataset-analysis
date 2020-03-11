import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.DoubleAccumulator;

import helpers.DatasetUtils;
import helpers.OutputUtils;
import scala.Tuple2;
import scala.Option;

public class Task02ReviewTable {
	
	private static String appName = "Task02ReviewTable";
	
	private static String uriReviewers = "src/main/resources/yelp_top_reviewers_with_reviews.csv";
	
	private static String outputDistinctUsers = "./output-02-a.csv";
	private static String outputAvarageCharacters = "./output-02-b.csv";
	private static String outputTopBusinesses = "./output-02-c.csv";
	private static String outputReviewsPerYear = "./output-02-d.csv";
	private static String outputFirstLastReview = "./output-02-e.csv";
	private static String outputPCC = "./output-02-f.csv";

	public static void main(String[] args) throws Exception {
		SparkConf config = new SparkConf().setAppName(appName).setMaster("local[*]");
		SparkContext sparkContext = new SparkContext(config);
		JavaSparkContext context = new JavaSparkContext(sparkContext);
		
		JavaRDD<String> rddReviewers = context.textFile(uriReviewers);
		
		JavaRDD<String> rddReviewersNoHeader = rddReviewers
				.mapPartitionsWithIndex(DatasetUtils.RemoveHeader, false);
		
        /* a) How many different users are in the dataset */
		
		Long rddReviewersDistinctCount = null;
		
		JavaRDD<String> rddReviewersUserID = rddReviewersNoHeader
				.map(row -> row.split("	")[1]);
		JavaRDD<String> rddReviewersDistinct = rddReviewersUserID.distinct();
		rddReviewersDistinctCount = rddReviewersDistinct.count();
		
		OutputUtils.writerInit(outputDistinctUsers);
        OutputUtils.writeLine(Arrays.asList(rddReviewersDistinctCount.toString()));
        OutputUtils.writerCleanup();

        /* b) What is the average number of characters in a user review */
		
		Double rddReviewersAvarage = null;
		
		JavaRDD<Double> rddReviewersReviewText = rddReviewersNoHeader
				.map(row -> Double.valueOf(DatasetUtils.decodeBase64(row.split("	")[3]).length()));
		rddReviewersAvarage = rddReviewersReviewText.reduce((accum, n) -> (accum + n));
		rddReviewersAvarage /= rddReviewersReviewText.count();
		
		OutputUtils.writerInit(outputAvarageCharacters);
        OutputUtils.writeLine(Arrays.asList(rddReviewersAvarage.toString()));
        OutputUtils.writerCleanup();
		
		/* c) What is the business id of top 10 businesses with the most number or reviews */
		
		JavaPairRDD<Long, String> rddBusinessIds = rddReviewersNoHeader
				.mapToPair(row -> new Tuple2<String, Long>(row.split("	")[2], 1L))
				.reduceByKey((a, b) -> a + b)
				.mapToPair(row -> new Tuple2<Long, String>(row._2, row._1))
				.sortByKey(false);
		
		OutputUtils.writerInit(outputTopBusinesses);
		rddBusinessIds.take(10).forEach(t -> {
			try {
				OutputUtils.writeLine(Arrays.asList(String.valueOf(t._2), String.valueOf(t._1)));
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		OutputUtils.writerCleanup();
		
		/* d) Find the number of reviews per year */
		
		JavaPairRDD<Long, Long> rddReviewsIdsYears = rddReviewersNoHeader
				.mapToPair(row -> new Tuple2<Long, Long>(DatasetUtils.ExtractYear(row.split("	")[4]), 1L))
				.reduceByKey((a, b) -> a + b);
		
		OutputUtils.writerInit(outputReviewsPerYear);
		rddReviewsIdsYears.collect().forEach(t -> {
			try {
				OutputUtils.writeLine(Arrays.asList(String.valueOf(t._1), String.valueOf(t._2)));
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		OutputUtils.writerCleanup();
		
		/* e) What is the time and date of first and last review */
		JavaRDD<Long> rddReviewsIdsDateTime = rddReviewersNoHeader
				.map(row -> Long.parseLong(DatasetUtils.ExtractTimestamp(row.split("	")[4])));
		
		Long minDateTime = null, maxDateTime = null;
		
		minDateTime = rddReviewsIdsDateTime.min(Comparator.<Long>naturalOrder());
		maxDateTime = rddReviewsIdsDateTime.max(Comparator.<Long>naturalOrder());
		
		OutputUtils.writerInit(outputFirstLastReview);
		OutputUtils.writeLine(Arrays.asList(DatasetUtils.ExtractDate(minDateTime)));
		OutputUtils.writeLine(Arrays.asList(DatasetUtils.ExtractDate(maxDateTime)));
		OutputUtils.writerCleanup();
		
		/* f) Calculate the Pearson correlation coefficient (PCC) */
		
		JavaPairRDD<String, Double> rddUserIdReviewCount = rddReviewersNoHeader
				.mapToPair(row -> new Tuple2<String, Double>(row.split("	")[1], 1D))
				.reduceByKey((a, b) -> a + b);
				
		JavaPairRDD<String, Double> rddUserIdReviewLength = rddReviewersNoHeader
				.mapToPair(row -> new Tuple2<String, Long>(row.split("	")[1], Long.valueOf(DatasetUtils.decodeBase64(row.split("	")[3]).length())))
				.groupByKey()
				.mapToPair(row -> new Tuple2<String, Double>(row._1, DatasetUtils.IteratorAverage(row._2)));
		
		JavaPairRDD<String, Tuple2<Double, Double>> rddUserIdJoined = rddUserIdReviewCount.join(rddUserIdReviewLength);
		
		Double rddUserIdReviewCountMean = rddUserIdReviewCount
				.map(row -> row._2)
				.reduce((accum, n) -> (accum + n));
		
		Double rddUserIdReviewLengthMean = rddUserIdReviewLength
				.map(row -> row._2)
				.reduce((accum, n) -> (accum + n));
		
		DoubleAccumulator accumUp = new DoubleAccumulator();
		DoubleAccumulator accumDownLeft = new DoubleAccumulator();
		DoubleAccumulator accumDownRight = new DoubleAccumulator();
		accumUp.register(sparkContext, Option.apply("accumUp"), false);
		accumDownLeft.register(sparkContext, Option.apply("accumDownLeft"), false);
		accumDownRight.register(sparkContext, Option.apply("accumDownRight"), false);
		
		rddUserIdJoined.foreach(element -> {
			accumUp.add((element._2._1 - rddUserIdReviewCountMean) * (element._2._2 - rddUserIdReviewLengthMean));
			accumDownLeft.add((element._2._1 - rddUserIdReviewCountMean) * (element._2._1 - rddUserIdReviewCountMean));
			accumDownRight.add((element._2._2 - rddUserIdReviewLengthMean) * (element._2._2 - rddUserIdReviewLengthMean));
		});
		
		Double accum = accumUp.value();
		Double accumDownL = Math.sqrt(accumDownLeft.value());
		Double accumDownR = Math.sqrt(accumDownRight.value());
		Double accumResult = accum / (accumDownL * accumDownR);
		
		OutputUtils.writerInit(outputPCC);
		OutputUtils.writeLine(Arrays.asList(accumResult.toString()));
		OutputUtils.writerCleanup();
		
		cleanup(context);
	}

	private static void cleanup(JavaSparkContext context) {
		context.close();
	}
	
	private static void debugRDD(JavaRDD<String> rdd) { 
		for(String line:rdd.collect())
            System.out.println(line);
	}
	
	private static void debugPairRDD(JavaPairRDD<String, Long> rdd) {
		rdd.foreach(data -> {
	        System.out.println(data._1() + " " + data._2());
	    }); 
	}
}