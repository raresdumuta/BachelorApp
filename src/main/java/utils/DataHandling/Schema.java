package utils.DataHandling;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Struct;

public class Schema {


    public static StructType reviewSchema() {
        StructField[] structFields = {
                new StructField("marketplace", DataTypes.StringType, true, Metadata.empty()),
                new StructField("product_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("review_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("customer_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("product_parent", DataTypes.StringType, true, Metadata.empty()),
                new StructField("product_title", DataTypes.StringType, true, Metadata.empty()),
                new StructField("product_category", DataTypes.StringType, true, Metadata.empty()),
                new StructField("star_rating", DataTypes.StringType, true, Metadata.empty()),
                new StructField("helpful_votes", DataTypes.StringType, true, Metadata.empty()),
                new StructField("total_votes", DataTypes.StringType, true, Metadata.empty()),
                new StructField("vine", DataTypes.StringType, true, Metadata.empty()),
                new StructField("verified_purchase", DataTypes.StringType, true, Metadata.empty()),
                new StructField("review_headline", DataTypes.StringType, true, Metadata.empty()),
                new StructField("review_body", DataTypes.StringType, true, Metadata.empty()),
                new StructField("review_date", DataTypes.StringType, true, Metadata.empty())};

        return new StructType(structFields);
    }


    public static StructType loyalCustomersSchema() {
        StructField[] structFields = {
                new StructField("customer_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("nrOfOrders", DataTypes.LongType, true, Metadata.empty()),
        };
        return new StructType(structFields);
    }

    public static StructType categoriesAndCountSchema(){
        StructField[] structFields = {
                new StructField("product_category", DataTypes.StringType, true, Metadata.empty()),
                new StructField("total_numbers_of_products", DataTypes.LongType, true, Metadata.empty()),
        };
        return new StructType(structFields);
    }

    public static StructType reviewsAndScore(){
        StructField[] structFields = {
                new StructField("product_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("average_score", DataTypes.StringType, true, Metadata.empty()),
        };
        return new StructType(structFields);
    }
}
