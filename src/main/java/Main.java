import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/* Testing playground class */

public class Main {
	
	public static void main(String[] args) {
		
		final SparkSession sparkSession = SparkSession.builder()
				.appName("TaskPlayground")
				.master("local[*]")
				.getOrCreate();

		final DataFrameReader dataFrameReader = sparkSession.read();
		dataFrameReader.option("header", "true");
		
		final Dataset<Row> csvDataFrame = dataFrameReader.csv("src/main/resources/yelp_businesses.csv");

		// Print Schema for column names and metadata
		csvDataFrame.printSchema();

	}

}
