package utils.DataHandling;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;


public class DataHandler {
    public static Dataset<Row> read(SparkSession sparkSession, String inputPath) {
        return sparkSession.read().option("header", "true").option("delimiter", "\t").csv(inputPath);
    }

    public static void write(SparkSession sparkSession, JavaRDD<Review> outputData, String outputPath) {

        JavaRDD<Row> rowOutputData = outputData.map(RowToEntityConverter::entityToRow);
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

        StructType structType = new StructType(structFields);
        Dataset<Row> output = sparkSession.createDataFrame(rowOutputData, structType);
        output.write().mode("overwrite").csv(outputPath);
    }

    public static void writeLoyalCustomers(SparkSession sparkSession, JavaRDD<Tuple2<String, Long>> outputData, String outputPath) {
        SQLContext sqlContext = new SQLContext(sparkSession);
        JavaRDD<Row> rowOutputData = outputData.map(RowToEntityConverter::loyalCustomerToRow);
        StructField[] structFields = {
                new StructField("customer_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("nrOfOrders", DataTypes.LongType, true, Metadata.empty()),
                };
        StructType structType = new StructType(structFields);
        Dataset<Row> output = sqlContext.createDataFrame(rowOutputData,structType);
        output.write().mode("overwrite").csv(outputPath);
    }

    public static void writeCategoryStats(SparkSession sparkSession, JavaRDD<Tuple2<String, Long>> outputData, String outputPath) {
        SQLContext sqlContext = new SQLContext(sparkSession);
        JavaRDD<Row> rowOutputData = outputData.map(RowToEntityConverter::loyalCustomerToRow);
        StructField[] structFields = {
                new StructField("product_category", DataTypes.StringType, true, Metadata.empty()),
                new StructField("total numbers of products", DataTypes.LongType, true, Metadata.empty()),
        };
        StructType structType = new StructType(structFields);
        Dataset<Row> output = sqlContext.createDataFrame(rowOutputData,structType);
        output.write().mode("overwrite").csv(outputPath);
    }


}