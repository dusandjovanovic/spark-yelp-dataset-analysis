import java.io.FileOutputStream;
import java.io.PrintStream;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import helpers.Schemas;

public class Task05DBase {

	private static String appName = "Task05DBase";
	
	private static String uriBusinesses = "src/main/resources/yelp_businesses.csv";
	private static String uriReviewers = "src/main/resources/yelp_top_reviewers_with_reviews.csv";
	private static String uriGraph = "src/main/resources/yelp_top_users_friendship_graph.csv";
	
	private static String output = "./output-05.txt";

	public static void main(String[] args) throws Exception {
		
		final SparkSession sparkSession = SparkSession.builder().appName(appName).master("local[*]")
				.getOrCreate();
		
		final DataFrameReader dataFrameReader = sparkSession.read();
		dataFrameReader.option("header", "true");
		
		final Dataset<Row> dataFrameBusinesses = dataFrameReader
				.schema(Schemas.schemaBusinesses)
				.csv(uriBusinesses);
		
		final Dataset<Row> dataFrameReviewers = dataFrameReader
				.schema(Schemas.schemaReviewers)
				.csv(uriReviewers);
		
		final Dataset<Row> dataFrameGraphs = dataFrameReader
				.schema(Schemas.schemaGraph)
				.csv(uriGraph);
		
		PrintStream out = new PrintStream(
		        new FileOutputStream(output, true), true);
		System.setOut(out);
		
		dataFrameBusinesses.printSchema();
		dataFrameReviewers.printSchema();
		dataFrameGraphs.printSchema();
	}
}
