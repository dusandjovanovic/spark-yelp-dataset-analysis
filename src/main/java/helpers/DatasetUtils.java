package helpers;

import java.util.Iterator;

import org.apache.spark.api.java.function.Function2;

public class DatasetUtils {
	
	private static DatasetUtils single_instance = null;
	
	public static DatasetUtils getInstance() 
    { 
        if (single_instance == null) 
            single_instance = new DatasetUtils(); 
  
        return single_instance; 
    } 
	
	public static Function2 RemoveHeader = new Function2<Integer, Iterator<String>, Iterator<String>>() {
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
}