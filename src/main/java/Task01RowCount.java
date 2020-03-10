import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import helpers.DatasetUtils;
import helpers.OutputUtils;

public class Task01RowCount {
	
	private static String uriBusinesses = "src/main/resources/yelp_businesses.csv";
	private static String uriReviewers = "src/main/resources/yelp_top_reviewers_with_reviews.csv";
	private static String uriGraph = "src/main/resources/yelp_top_users_friendship_graph.csv";
	
	private static String[] outputHeader = {"dataset_name", "row_count"};
	private static String columnBusinesses = "yelp_businesses";
	private static String columnReviewers = "yelp_top_reviewers_with_reviews";
	private static String columnGraph = "yelp_top_users_friendship_graph";
	
	private static String output = "./output.csv";

	public static void main(String[] args) throws Exception {
		SparkConf config = new SparkConf().setAppName("Task01RowCount").setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(config);
		
		JavaRDD<String> rddBusinesses = context.textFile(uriBusinesses);
		JavaRDD<String> rddReviewers = context.textFile(uriReviewers);
		JavaRDD<String> rddGraph = context.textFile(uriGraph);
		
		Long rddBusinessesRows, rddReviewersRows, rddGraphRows;
		
		rddBusinesses = rddBusinesses.mapPartitionsWithIndex(DatasetUtils.RemoveHeader, false);
		rddReviewers = rddReviewers.mapPartitionsWithIndex(DatasetUtils.RemoveHeader, false);
		rddGraph = rddGraph.mapPartitionsWithIndex(DatasetUtils.RemoveHeader, false);
		
		rddBusinessesRows = rddBusinesses.count();
		rddReviewersRows = rddReviewers.count();
		rddGraphRows = rddGraph.count();
		
        OutputUtils.initWrite(output);
        OutputUtils.writeLine(Arrays.asList(outputHeader));
        OutputUtils.writeLine(Arrays.asList(columnBusinesses, rddBusinessesRows.toString()));
        OutputUtils.writeLine(Arrays.asList(columnReviewers, rddReviewersRows.toString()));
        OutputUtils.writeLine(Arrays.asList(columnGraph, rddGraphRows.toString()));
        OutputUtils.cleanupWrite();
		
		cleanup(context);
	}

	private static void cleanup(JavaSparkContext context) {
		context.close();
	}
}
