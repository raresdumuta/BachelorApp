package main;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.ArgumentContainer;

import utils.DataHandling.DataHandler;
import utils.DataHandling.Review;
import utils.DataHandling.ReviewConverter;

import java.util.Objects;

public class Job {
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);

        ArgumentContainer argumentContainer = new ArgumentContainer(args);
        String inputPath = argumentContainer.getRequired("input");
        String outputPath = argumentContainer.getRequired("output");

        Dataset<Row> rowDataset = DataHandler.read(sparkSession, inputPath);
        JavaRDD<Review> reviewRDD = rowDataset.toJavaRDD()
                .map(ReviewConverter::rowToEntity).filter(Objects::nonNull);

        EnhancementApplication enhancementApplication = new EnhancementApplication(reviewRDD);
        JavaRDD<Review> lastYearReviews = enhancementApplication.lastYearReviews();

        JavaRDD<Tuple2<String,Long>> categoryStats = enhancementApplication.getCategoryStats()
                .map(entry -> new Tuple2<>(entry._1(),entry._2()));
        Long totalNumberOfReviewsAnalyzed =  enhancementApplication.getTotalNumberOfReviews();

        JavaRDD<Tuple2<String, Long>> loyalCustomers = enhancementApplication.getLoyalCustomers(5L)
                .map(entry -> new Tuple2<>(entry._1(),entry._2()));

        DataHandler.write(sqlContext,lastYearReviews,outputPath + "/lastYearReviews");
        DataHandler.writeLoyalCustomers(sqlContext,loyalCustomers,outputPath + "/loyalCustomers");
        DataHandler.writeCategoryStats(sqlContext,categoryStats,outputPath + "/categoryStats");


        System.out.println("total number of reviews is: " + totalNumberOfReviewsAnalyzed);
        sparkSession.close();
    }
}
