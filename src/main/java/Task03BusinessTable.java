import java.io.IOException;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import helpers.DatasetUtils;
import helpers.OutputUtils;
import scala.Tuple2;

public class Task03BusinessTable {
	
	private static String appName = "Task03BusinessTable";
	
	private static String uriBusinesses = "src/main/resources/yelp_businesses.csv";
	
	private static String outputAvarageRating = "./output-03-a.csv";
	private static String outputTopCategories = "./output-03-b.csv";
	private static String outputGeographicalCentroid = "./output-03-c.csv";

	public static void main(String[] args) throws Exception {
		SparkConf config = new SparkConf().setAppName(appName).setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(config);
		
		JavaRDD<String> rddBusinesses = context.textFile(uriBusinesses);
		
		JavaRDD<String> rddBusinessesNoHeader = rddBusinesses
				.mapPartitionsWithIndex(DatasetUtils.RemoveHeader, false);
		
        /* a) What is the average rating for businesses in each city */
		
		JavaPairRDD<String, Double> rddBusinessesCity = rddBusinessesNoHeader
				.mapToPair(row -> new Tuple2<String, Long>(row.split("	")[3], Long.valueOf(row.split("	")[8])))
				.groupByKey()
				.mapToPair(row -> new Tuple2<String, Double>(row._1, DatasetUtils.IteratorAverage(row._2)));
		
		OutputUtils.writerInit(outputAvarageRating);
		rddBusinessesCity.collect().forEach(t -> {
			try {
				OutputUtils.writeLine(Arrays.asList(String.valueOf(t._1), String.valueOf(t._2)));
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
        OutputUtils.writerCleanup();
        
        /* b) What are the top 10 most frequent categories in the data */
		
		JavaPairRDD<Long, String> rddBusinessesCategories = rddBusinessesNoHeader
				.map(row -> row.split("	")[10])
				.flatMap(row -> Arrays.asList(row.split(", ")).iterator())
				.mapToPair(row -> new Tuple2<String, Long>(row, 1L))
				.reduceByKey((a, b) -> a + b)
				.mapToPair(row -> new Tuple2<Long, String>(row._2, row._1))
				.sortByKey(false);
		
		OutputUtils.writerInit(outputTopCategories);
		rddBusinessesCategories.take(10).forEach(t -> {
			try {
				OutputUtils.writeLine(Arrays.asList(String.valueOf(t._2), String.valueOf(t._1)));
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		OutputUtils.writerCleanup();
		
		/* c) Calculate the geographical centroid of each region by postal-code */
		
		JavaPairRDD<String, Tuple2<Double, Double>> rddBusinessesPostalCode = rddBusinessesNoHeader
				.mapToPair(row -> new Tuple2<String, Tuple2<Double, Double>>(row.split("	")[5], new Tuple2<Double, Double>(Double.valueOf(row.split("	")[6]), Double.valueOf(row.split("	")[7]))))
				.groupByKey()
				.mapToPair(row -> new Tuple2<String, Tuple2<Double, Double>>(row._1, DatasetUtils.IteratorGeographicalCentroid(row._2)));
		
		OutputUtils.writerInit(outputGeographicalCentroid);
		rddBusinessesPostalCode.collect().forEach(t -> {
			try {
				OutputUtils.writeLine(Arrays.asList(String.valueOf(t._1), String.valueOf(t._2)));
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		OutputUtils.writerCleanup();

		cleanup(context);
	}

	private static void cleanup(JavaSparkContext context) {
		context.close();
	}
	
	private static void debugRDD(JavaRDD<Integer> rdd) { 
		for(Integer line:rdd.collect())
            System.out.println(line);
	}
	
	private static void debugPairRDD(JavaPairRDD<String, Long> rdd) {
		rdd.foreach(data -> {
	        System.out.println(data._1() + " " + data._2());
	    }); 
	}
}
