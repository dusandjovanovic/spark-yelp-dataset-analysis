# SPARK: Yelp dataset analysis

## Project structure

Each tasks consists of a **separate runnable java class** and all associted code is located in the `.java` files which will be explained. Dataset files are placed in the `resources` folder and loaded locally.

The location of **output files** is the `outputs/` folder. Output files are named accordingly to the coresponding task such as `output-01.csv` is the output file of the first task.

```
/
  src/
    ...
    helpers/
        DatasetUtils.java
        OutputUtils.java
	Schemas.java
    <default_package>/
        Main.java
        Task01RowCount.java
        Task02ReviewTable.java
        Task03BusinessTable.java
        Task04GraphTable.java
        Task05DBase.java
        Task06DBase.java
  target/
  outputs/
  .cache-main
  .classpath
  .project
  pom.xml
```

For example, the class which could be found in `Task01RowCount.java` operates everything related to the first task. All other task-related classes are in the `<default_package>`, on the other hand the `helpers` package has three utility classes with shared behaviours.

```diff
- Important* resource files are not present on this repository since they are 1GB+ in size!

+ All dataset resources should be imported in the /resources folder.
```

## Shared behaviours and classes

`DatasetUtils` class has several shared and isolated methods such as:
1) `String decodeBase64` - decoding base64 strings
2) `Long ExtractYear` - extracting year from timestamp
3) `String ExtractTimestamp` - extracting and trimming timestamp strings
4) `String ExtractDate` - extracting date object from timestamp
5) `Double IteratorAverage` - finding the average value from an Iterator
6) `Tuple2<Double, Double> IteratorGeographicalCentroid` - finding the centroid location
7) `Function2<Integer, Iterator<String>, Iterator<String>> RemoveHeader` - clearing header rows from .csv files
8) `FlatMapFunction<String, String> RemoveSpaces` - clearing spaces in strings

In this report only signatures of mentioned methods are shown. Source code could be inspected for a detailed look.

```java
package helpers;

public class DatasetUtils {
	
	private static DatasetUtils single_instance = null;
	
	public static DatasetUtils getInstance();
	
	public static String decodeBase64(String bytes);
	
	public static Long ExtractYear(String stamp);
	
	public static String ExtractTimestamp(String stamp);
	
	public static String ExtractDate(Long timestamp);
	
	public static Double IteratorAverage(Iterable<Long> iter);
	
	public static Tuple2<Double, Double> IteratorGeographicalCentroid(Iterable<Tuple2<Double, Double>> iter);
	
	public static Function2<Integer, Iterator<String>, Iterator<String>> RemoveHeader = new Function2<Integer, Iterator<String>, Iterator<String>>();
	
	public static FlatMapFunction<String, String> RemoveSpaces = new FlatMapFunction<String, String>();
}
```

Similarly, the `OutputUtils` class encapsulates methods for writing and comosing .csv files as output. Important methods are:
1) `void writerInit` - opening a new file-stream by given path
2) `void writerCleanup` - closing the stream
3) `void writeLine` - writing a new line to the file

```java
package helpers;

public class OutputUtils {
    
    public static void writerInit(String output) throws IOException;
    
    public static void writerCleanup() throws IOException;
    
    public static void writeLine(List<String> values) throws IOException;

    public static void writeLine(List<String> values, char separators) throws IOException;

    private static String followCVSformat(String value);

    public static void writeLine(List<String> values, char separators, char customQuote) throws IOException;
}
```

Lastly, `Schemas` is a static class holding database `StructType` schemas used in last two tasks.

```java
package helpers;

import org.apache.spark.sql.types.StructType;

public class Schemas {

	public static StructType schemaBusinesses = new StructType()
	    .add("business_id", "string")
	    .add("name", "string")
	    .add("address", "string")
	    ...
	
	public static StructType schemaReviewers = new StructType()
	    .add("review_id", "string")
	    .add("user_id", "string")
	    .add("business_id", "string")
	    ...
	
	public static StructType schemaGraph = new StructType()
	    .add("src_user_id", "string")
	    .add("dst_user_id", "string");
}
```

## Task 01

### a) Load the dataset into separate RDDs and cont the number of rows in each RDD

```java
public static void main(String[] args) throws Exception {
	SparkConf config = new SparkConf().setAppName(appName).setMaster("local[*]");
	JavaSparkContext context = new JavaSparkContext(config);

	JavaRDD<String> rddBusinesses = context.textFile(uriBusinesses);
	JavaRDD<String> rddReviewers = context.textFile(uriReviewers);
	JavaRDD<String> rddGraph = context.textFile(uriGraph);

	Long rddBusinessesRows, rddReviewersRows, rddGraphRows;

	JavaRDD<String> rddBusinessesNoHeader = rddBusinesses
		.mapPartitionsWithIndex(DatasetUtils.RemoveHeader, false);
	JavaRDD<String> rddReviewersNoHeader = rddReviewers
		.mapPartitionsWithIndex(DatasetUtils.RemoveHeader, false);
	JavaRDD<String> rddGraphNoHeader = rddGraph
		.mapPartitionsWithIndex(DatasetUtils.RemoveHeader, false);
	...
}
```

In the beginning, Spark envirement should be set-up and context initialised. Afterwards **separate RDDs** are formed for every input file and cleared of the first row which has mandatory .csv meta headers - for this the shared `DatasetUtils.RemoveHeader` method is being used.

**This method is used in every task to generate a `NoHeaders` RDD from the raw data RDD and hence won't be explained again in the document.**

```java
	rddBusinessesRows = rddBusinessesNoHeader.count();
	...
		
        OutputUtils.writerInit(output);
        OutputUtils.writeLine(Arrays.asList(columnsHeader));
        OutputUtils.writeLine(Arrays.asList(columnBusinesses, rddBusinessesRows.toString()));
	...
```

Lastly, finding the count of rows of every RDD is fairly easy using the `.count()` method from Spark's API.

The result can be found in the file `outputs/output-01.csv`.


## Task 02

### a) How many different users are in the dataset

```java
	Long rddReviewersDistinctCount = null;
		
	JavaRDD<String> rddReviewersUserID = rddReviewersNoHeader
		.map(row -> row.split("	")[1]);
	JavaRDD<String> rddReviewersDistinct = rddReviewersUserID.distinct();
	rddReviewersDistinctCount = rddReviewersDistinct.count();

	OutputUtils.writerInit(outputDistinctUsers);
        OutputUtils.writeLine(Arrays.asList(rddReviewersDistinctCount.toString()));
        OutputUtils.writerCleanup();
```

In order to find distinct users all **user_id** values should be mapped separately and afterwards distincted with `.distinct()`. 

The result can be found in the file `outputs/output-02-a.csv`.

### b) What is the average number of characters in a user review

```java
	Double rddReviewersAvarage = null;
		
	JavaRDD<Double> rddReviewersReviewText = rddReviewersNoHeader
		.map(row -> Double.valueOf(DatasetUtils.decodeBase64(row.split("	")[3]).length()));
	rddReviewersAvarage = rddReviewersReviewText.reduce((accum, n) -> (accum + n));
	rddReviewersAvarage /= rddReviewersReviewText.count();

	OutputUtils.writerInit(outputAvarageCharacters);
        OutputUtils.writeLine(Arrays.asList(rddReviewersAvarage.toString()));
        OutputUtils.writerCleanup();
```

Firstly, all user review string should be measured in length, in order for this to happen every encoded byte-string should be decoded using the helper method `DatasetUtils.decodeBase64`. Once encoded string are available, their lengths can be measured. After these values are **mapped** it is essential to to exectue an **action of `reduction`**  and accumulate all found values. Lastly, finding the average value requires using the total count of rows. 

The result can be found in the file `outputs/output-02-b.csv`.

### c) What is the business id of top 10 businesses with the most number or reviews

```java
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
```

Important information for this task is the **bussines_id** and the number of reviews associated to it, hence the mapping to a `JavaPairRDD<String, Long>` RDD which relies on the `Tuple2` object for every entry. After these tuples are made, **reduction by key** is used to accumulate reviews coming from the same business source. Lastly, sorting is done by the accumulated values - first by swapping the topuple's key and value and then sorting by value giving a `JavaPairRDD<Long, String>` RDD of businesses sorted in descending order by the number of reviews. Top 10 results are taken from the RDD relying on the `.take(n)` API.

The result can be found in the file `outputs/output-02-c.csv`.

### d) Find the number of reviews per year

```java
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
```

In order to find the number of reviews per year a pair mapping of `JavaPairRDD<Long, Long>` is being used. After the year and `1L` value are mapped for every entry these should be **reduced** by every year. This is a similar approach as used in one of the previous subtasks.

The result can be found in the file `outputs/output-02-d.csv`.

### e) What is the time and date of first and last review

```java
	JavaRDD<Long> rddReviewsIdsDateTime = rddReviewersNoHeader
		.map(row -> Long.parseLong(DatasetUtils.ExtractTimestamp(row.split("	")[4])));

	Long minDateTime = null, maxDateTime = null;

	minDateTime = rddReviewsIdsDateTime.min(Comparator.<Long>naturalOrder());
	maxDateTime = rddReviewsIdsDateTime.max(Comparator.<Long>naturalOrder());

	OutputUtils.writerInit(outputFirstLastReview);
	OutputUtils.writeLine(Arrays.asList(DatasetUtils.ExtractDate(minDateTime)));
	OutputUtils.writeLine(Arrays.asList(DatasetUtils.ExtractDate(maxDateTime)));
	OutputUtils.writerCleanup();
```

Finding the min/max values requires only mapping timestamp information of all entries. Once these are being mapped, `.min(IComparator)` and `.max(IComparator)` actions are used to extract these values. UNIX timestamps represent the number of miliseconds and shouldn't be converted for the `Comparator` instances. Lastly, helper methods are used to convert the timestamps to dates.

The result can be found in the file `outputs/output-02-e.csv`.

### f) Calculate the Pearson correlation coefficient (PCC)

```java
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
```

This task requires multiple APIs including the accumulator variables. Firstly, two different `JavaPairRDD<String, Double>` RDDs are formed to represent review lengths and review counts. `IteratorAverage` is the helper method used to find an **avarege value** over an `Itereable` collection. After, they are being joined into a single `JavaPairRDD<String, Tuple2<Double, Double>>` RDD where each user_id is linked with coresponding values. Mean values are found on top of these RDDs using the action of **reduction**. Afterwards, accumulators are made and registered - in total of three for **every part of the equation**. Then, using the safe-lock approach of accumulators being changed in the `foreach` traversal these values are formed. The only thing left is to follow the equation and combine accumulated values togther into a result.

The result can be found in the file `outputs/output-02-f.csv`.


## Task 03

### a) What is the average rating for businesses in each city

```java
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
```

Finding the average rating requires mapping all ratings and cities together in a `JavaPairRDD<String, Double>`, grouping the ratings by city and lastly using the `IteratorAverage` the helper method used to find an **avarege value** over an `Itereable` collection - where collection represent and ratings for one city.

The result can be found in the file `outputs/output-03-a.csv`.

### b) What are the top 10 most frequent categories in the data

```java
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
```

Category lists should be first mapped, afterwards flattened so that separate categories end up as single entries. Since this point, every category should be transformed into `JavaPairRDD<String, Long>` with it's name and '1L' repetitions. After reduction, repetitions are accumulated to all unique categories. Lastly, mapping the order around and sorting gives the categories and number of repetitions in descending order. Top 10 results are taken from the RDD relying on the `.take(n)` API. 

The result can be found in the file `outputs/output-03-b.csv`.

### c) Calculate the geographical centroid of each region by postal-code

```java
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
```

Finding the centroid coordinates for every postal code requires a form of `JavaPairRDD<String, Tuple2<Double, Double>>` where each postal code is associated with it's geographical centroid lat/lng. Once all entries are mapped with postal code and location, a **grouping by key** is essential to gather all related coordinate values (for each postal code). After the grouping, a `IteratorGeographicalCentroid` helper method is called for every `Iterable` collection of coordinates performing the required calculation and returning a `Touple` of values representing the searched point in the form of `Tuple2<Double, Double>`.

The result can be found in the file `outputs/output-03-c.csv`.


## Task 04

### a) Find the top 10 nodes with the most number of in and out degrees

```java
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
	
	// Writing to a file for Dst degrees is almost identical...
```

`JavaPairRDD<Long, String>` is required for two directions this time. Each RDD is formed separately, in the case of out and in edges. The approach is identical, first every id is mapped with a value of `1L` for every out-edge, and then in the second iteration for every in-edge. At this point, every node id is associated with a different instance of the degree and **reduction** should be made to count the number of edges for every id. After reduction, previously explained approach of swapping and then sorting by value is being used. Lastly, for both cases, top 10 results are taken from the RDD relying on the `.take(n)` API.

The results can be found in files `outputs/output-04-a-in.csv` and `outputs/output-04-a-out.csv` files for nodes with the most in/out degrees in the mentioned order.

### b) Find the mean and median for number of in and out degrees

```java
	Double meanSrc = new Double(0D);
	
	meanSrc = rddGraphSrc
		.map(row -> (double)row._1)
		.reduce((accum, n) -> (accum + n));
	meanSrc /= rddGraphSrc.count();
```

Finding the mean values is straightforward and relies on the previously formed RDDs, where only the values representing the number of degrees are extracted and then accumulated. In the end, the total count of values determines the mean value. Approach is identical for both in and out-edges.

```java
	Double median[] = { new Double(0D), new Double(0D) };
	...

	Long count = rddGraphSrc.count();
	Long index[] = { 0L, count / 2, 0L };
	if (count % 2 == 0)
		index[2] = 1L;

	rddGraphSrc.collect().forEach(t -> {
		if (index[2] == 1 && index[0] == index[1].longValue() - 1)
			median[0] += Double.valueOf(t._1);
		if (index[2] == 1 && index[0].longValue() == index[1].longValue()) {
			median[0] += Double.valueOf(t._1);
			median[0] /= 2;
		}
		if (index[2] == 0 && index[0].longValue() == index[1].longValue())
			median[0] = Double.valueOf(t._1);

		index[0]++;
	});
```

Finding the median value relies on the `collect()` and traversing every entry while extracting only the values of the correct index. The `.count()` gives the sufficent information on how to approach the calculation of a median value.

Median value algorithm is naive and relies on the simple traversal where an element of **targeted index (middle of RDD)** is being extracted, or two elements and their mean value in case of an even number of RDD-entries. Thereby, this algorith will find *the-most-middle element in the RDD* which has been previously sorted for the needs of subtask a).

The result can be found in the file `outputs/output-04-b.csv`.


## Task 05

### Load each file in the dataset into seprate DataFrames. Keep column types and names as in dataset description.

```java
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
```

In order to form a **DataFrame** a `SparkSession` should be initialised. On top of the created session, DataFrames can be formed relying on a resource and a **custom-defined `Schema Object`**.

```java
public class Schemas {

	public static StructType schemaBusinesses = new StructType()
	    .add("business_id", "string")
	    .add("name", "string")
	    .add("address", "string")
	    .add("city", "string")
	    .add("state", "string")
	    .add("postal_code", "string")
	    .add("latitude", "float")
	    .add("longitude", "float")
	    .add("stars", "float")
	    .add("review_count", "integer")
	    .add("categories", "string");
    	...
```

These `Schema` objects are defined in the helper static class `Schemas`, separate object is present for every resource gicing a three in total. In the code above we can see the example schema representing the Business's table.

Lastly, every schema is printed and saved in a simple text file. The result can be found in the file `outputs/output-05.txt`.

