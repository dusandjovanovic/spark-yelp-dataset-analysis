import java.io.IOException;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import helpers.DatasetUtils;
import helpers.OutputUtils;
import scala.Tuple2;

public class Task04GraphTable {
	
	private static String appName = "Task04GraphTable";
	
	private static String uriGraph = "src/main/resources/yelp_top_users_friendship_graph.csv";
	
	private static String[] columnsHeader = {"operation", "value"};
	private static String outputTopNodesDst = "./output-04-a-in.csv";
	private static String outputTopNodesSrc = "./output-04-a-out.csv";
	private static String outputMeanMedian = "./output-04-b.csv";

	public static void main(String[] args) throws Exception {
		SparkConf config = new SparkConf().setAppName(appName).setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(config);
		
		JavaRDD<String> rddGraph = context.textFile(uriGraph);
		
		JavaRDD<String> rddGraphNoHeader = rddGraph
				.mapPartitionsWithIndex(DatasetUtils.RemoveHeader, false);
		
        /* a) Find the top 10 nodes with the most number of in and out degrees */
		
		JavaPairRDD<Long, String> rddGraphSrc = rddGraphNoHeader
				.mapToPair(row -> new Tuple2<String, Long>(row.split(",")[0], 1L))
				.reduceByKey((a, b) -> a + b)
				.mapToPair(row -> new Tuple2<Long, String>(row._2, row._1))
				.sortByKey(false);
		
		JavaPairRDD<Long, String> rddGraphDst = rddGraphNoHeader
				.mapToPair(row -> new Tuple2<String, Long>(row.split(",")[1], 1L))
				.reduceByKey((a, b) -> a + b)
				.mapToPair(row -> new Tuple2<Long, String>(row._2, row._1))
				.sortByKey(false);
		
		OutputUtils.writerInit(outputTopNodesSrc);
		rddGraphSrc.take(10).forEach(t -> {
			try {
				OutputUtils.writeLine(Arrays.asList(String.valueOf(t._2), String.valueOf(t._1)));
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		OutputUtils.writerCleanup();
		
		OutputUtils.writerInit(outputTopNodesDst);
		rddGraphDst.take(10).forEach(t -> {
			try {
				OutputUtils.writeLine(Arrays.asList(String.valueOf(t._2), String.valueOf(t._1)));
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		OutputUtils.writerCleanup();
		
		
		/* b) Find the mean and median for number of in and out degrees */
		
		Double meanSrc = new Double(0D);
		Double meanDst = new Double(0D);
		Double median[] = { new Double(0D), new Double(0D) };
		
		meanSrc = rddGraphSrc
				.map(row -> (double)row._1)
				.reduce((accum, n) -> (accum + n));
		meanSrc /= rddGraphSrc.count();
		
		meanDst = rddGraphDst
				.map(row -> (double)row._1)
				.reduce((accum, n) -> (accum + n));
		meanDst /= rddGraphDst.count();
		
		Long count = rddGraphSrc.count();
		Long index[] = { 0L, count / 2, 0L };
		if (count % 2 != 0)
			index[2] = 1L;

		rddGraphSrc.collect().forEach(t -> {
			if (index[2] == 1 && index[0] == index[1] - 1)
				median[0] += t._1;
			else if (index[2] == 1 && index[0] == index[1]) {
				median[0] += t._1;
				median[0] /= 2;
			}
			else if (index[2] == 0 && index[0] == index[1])
				median[0] /= t._1;
			
			index[0]++;
		});
		
		count = rddGraphDst.count();
		index[0] = 0L;
		index[1] = count / 2;
		index[2] = 0L;
		if (count % 2 != 0)
			index[2] = 1L;
		
		rddGraphDst.collect().forEach(t -> {
			if (index[2] == 1 && index[0] == index[1] - 1)
				median[1] += t._1;
			else if (index[2] == 1 && index[0] == index[1]) {
				median[1] += t._1;
				median[1] /= 2;
			}
			else if (index[2] == 0 && index[0] == index[1])
				median[1] /= t._1;
			
			index[0]++;
		});
		
		OutputUtils.writerInit(outputMeanMedian);
        OutputUtils.writeLine(Arrays.asList(columnsHeader));
        OutputUtils.writeLine(Arrays.asList("mean_src", meanSrc.toString()));
        OutputUtils.writeLine(Arrays.asList("mean_dst", meanDst.toString()));
        OutputUtils.writeLine(Arrays.asList("median_src", median[0].toString()));
        OutputUtils.writeLine(Arrays.asList("median_dst", median[1].toString()));
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
