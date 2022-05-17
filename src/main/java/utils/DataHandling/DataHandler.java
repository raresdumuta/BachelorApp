package utils.DataHandling;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import scala.Tuple2;


public class DataHandler {

    public static Dataset<Row> read(SparkSession sparkSession, String inputPath) {
        return sparkSession.read().option("header", "true").option("delimiter", "\t").csv(inputPath);
    }

    public static void write(SQLContext sqlContext, JavaRDD<Review> outputData, String outputPath) {

        JavaRDD<Row> rowOutputData = outputData.map(ReviewConverter::entityToRow);

        Dataset<Row> output = sqlContext.createDataFrame(rowOutputData, Schema.reviewSchema());
        output.write().mode("overwrite").csv(outputPath);
    }

    public static void writeLoyalCustomers(SQLContext sqlContext, JavaRDD<Tuple2<String, Long>> outputData, String outputPath) {

        JavaRDD<Row> rowOutputData = outputData.map(ReviewConverter::loyalCustomerToRow);
        Dataset<Row> output = sqlContext.createDataFrame(rowOutputData,Schema.loyalCustomersSchema());
        output.write().mode("overwrite").csv(outputPath);
    }

    public static void writeCategoryStats(SQLContext sqlContext, JavaRDD<Tuple2<String, Long>> outputData, String outputPath) {
        JavaRDD<Row> rowOutputData = outputData.map(ReviewConverter::loyalCustomerToRow);

        Dataset<Row> output = sqlContext.createDataFrame(rowOutputData,Schema.categoriesAndCountSchema());
        output.write().mode("overwrite").csv(outputPath);
    }


}