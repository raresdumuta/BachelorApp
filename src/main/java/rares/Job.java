package rares;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.ArgumentContainer;

import utils.DataHandling.DataHandler;
import utils.DataHandling.Review;
import utils.DataHandling.RowToEntityConverter;

import java.util.Objects;

public class Job {
    public static void main(String[] args){
        ArgumentContainer argumentContainer = new ArgumentContainer(args);


        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        String inputPath = argumentContainer.getRequired("input");
        String outputPath = argumentContainer.getRequired("output");

        Dataset<Row> inputDataSet = DataHandler.read(sparkSession, inputPath);
        JavaRDD<Review> inputData = inputDataSet.toJavaRDD()
                .map(RowToEntityConverter::rowToEntity).filter(Objects::nonNull);

        EnhancementApplication enhancementApplication = new EnhancementApplication(inputData);
        JavaRDD<Review> lastYearReviews = enhancementApplication.lastYearReviews();
        DataHandler.write(sparkSession,lastYearReviews,outputPath + "/lastYearReviews");

        JavaRDD<Tuple2<String,Long>> categoryStats = enhancementApplication.getCategoryStats()
                .map(entry -> new Tuple2<>(entry._1(),entry._2()));
        Long totalNumberOfReviewsAnalyzed =  enhancementApplication.getTotalNumberOfReviews();

        JavaRDD<Tuple2<String, Long>> loyalCustomers = enhancementApplication.getLoyalCustomers(5L)
                .map(entry -> new Tuple2<>(entry._1(),entry._2()));
        DataHandler.writeLoyalCustomers(sparkSession,loyalCustomers,outputPath + "/loyalCustomers");

//        FileWriter fileWriter = new FileWriter(outputPath + "/totalReviews.txt");
//        fileWriter.write(totalNumberOfReviewsAnalyzed.toString());
//        fileWriter.close();

        DataHandler.writeCategoryStats(sparkSession,categoryStats,outputPath + "/categoryStats");
        System.out.println("total number of reviews is: " + totalNumberOfReviewsAnalyzed);
        sparkSession.close();
    }
}
