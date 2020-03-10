# SPARK: Yelp dataset analysis

## Project structure

Each tasks consists of a **separate runnable java class** and all associted code is located in the `.java` files which will be explained. Dataset files are placed in the `resources` folder and loaded locally.

```
/
  src/
    ...
    helpers/
        DatasetUtils.java
        OutputUtils.java
    <default_package>
        Main.java
        Task01RowCount.java
        Task02ReviewTable.java
        Task03BusinessTable.java
        Task04GraphTable.java
        Task05DBase.java
        Task06DBase.java
  .settings/
  target/
  .cache-main
  .classpath
  .project
  pom.xml
```

For example, the class which could be found in `Task01RowCount.java` operates everything related to the first task. All other task-related classes are in the `<default_package>`, on the other hand the `helpers` package has two utility classes with shared behaviours.

## Shared behaviours and classes

`DatasetUtils` class has several shared and isolated methods such as:
1) `Long ExtractYear` - extracting year from timestamp
2) `String ExtractTimestamp` - extracting and trimming timestamp strings
3) `String ExtractDate` - extracting date object from timestamp
4) `Double IteratorAverage` - finding the average value from an Iterator
5) `Tuple2<Double, Double> IteratorGeographicalCentroid` - finding the centroid location
6) `Function2<Integer, Iterator<String>, Iterator<String>> RemoveHeader` - clearing header rows from .csv files
7) `FlatMapFunction<String, String> RemoveSpaces` - clearing spaces in strings

```java
package helpers;

public class DatasetUtils {
	
	private static DatasetUtils single_instance = null;
	
	public static DatasetUtils getInstance() { 
        if (single_instance == null) 
            single_instance = new DatasetUtils(); 
  
        return single_instance; 
    }
	
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
