package helpers;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.codec.binary.Base64;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class DatasetUtils {
	
	private static DatasetUtils single_instance = null;
	
	public static DatasetUtils getInstance() { 
        if (single_instance == null) 
            single_instance = new DatasetUtils(); 
  
        return single_instance; 
    }
	
	public static String decodeBase64(String bytes) {
		String byteString = new String(Base64.decodeBase64(bytes.getBytes()));
		return byteString;
	}
	
	public static Long ExtractYear(String stamp) {
		String subString = ExtractTimestamp(stamp);

		Instant instant = Instant.ofEpochSecond(Long.parseLong(subString));
		LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
		return Long.valueOf(ldt.getYear());
	}
	
	public static String ExtractTimestamp(String stamp) {
		int iend = stamp.indexOf(".");
		String subString = stamp;
		if (iend != -1)
		    subString= stamp.substring(0 , iend);
		
		return subString;
	}
	
	public static String ExtractDate(Long timestamp) {
		Instant instant = Instant.ofEpochSecond(timestamp);
		LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
		DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
		return ldt.format(formatter);
	}
	
	public static Double IteratorAverage(Iterable<Long> iter) {
		Iterator<Long> iterator = iter.iterator();
		Double _result = new Double(iterator.next());
		Double counter = new Double(1);
		
        for(Long next : iter) {
        	_result += next;
        	counter++;
        }
        
        return _result / counter;
	}
	
	public static Tuple2<Double, Double> IteratorGeographicalCentroid(Iterable<Tuple2<Double, Double>> iter) {
		Iterator<Tuple2<Double, Double>> iterator = iter.iterator();
		Double counter = new Double(1);
		Double latitude = new Double(0);
		Double longitude = new Double(0);
		
        for(Tuple2<Double, Double> next : iter) {
        	latitude += next._1;
        	longitude += next._2;
        	counter++;
        }
        
        return new Tuple2<Double, Double>(latitude/counter, longitude/counter);
	}
	
	public static Function2<Integer, Iterator<String>, Iterator<String>> RemoveHeader = new Function2<Integer, Iterator<String>, Iterator<String>>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = -3387174225125238972L;

		public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
	        if(index == 0 && iterator.hasNext()) {
	            iterator.next();
	            return iterator;
	        }
	        else
	            return iterator;
	    }
	};
	
	public static FlatMapFunction<String, String> RemoveSpaces = new FlatMapFunction<String, String>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4519544274002840432L;

		public Iterator<String> call(String row) {
			return Arrays.asList(row.split(" ")).iterator();
		}
	};
}