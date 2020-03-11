package helpers;

import org.apache.spark.sql.types.StructType;

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
	
	public static StructType schemaReviewers = new StructType()
			.add("review_id", "string")
		    .add("user_id", "string")
		    .add("business_id", "string")
		    .add("review_text", "string")
		    .add("review_date", "timestamp");
	
	public static StructType schemaGraph = new StructType()
		    .add("src_user_id", "string")
		    .add("dst_user_id", "string");
}
