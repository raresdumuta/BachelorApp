import main.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import utils.DataHandling.DataHandler;
import utils.DataHandling.Review;
import utils.DataHandling.ReviewConverter;

import java.time.LocalDate;

import static junit.framework.Assert.assertTrue;


public class JobTest {
    JavaRDD<Tuple2<String, String>> productsAndScores;
    JavaRDD<Review> lastYearReviews;
    JavaRDD<Tuple2<String, Long>> categoryStats;
    JavaRDD<Tuple2<String, Long>> loyalCustomers;

    @Before
    public void testReadWrite() {
        String inputPath = "src/test/resources/input.tsv";
        String output = "src/test/resources/output";

        String[] args = new String[]{
                "input=" + inputPath,
                "output=" + output
        };
        Job.main(args);
        String productsScoreInputPath = "src/test/resources/output/productsScore";
        String lastYearReviewsPath = "src/test/resources/output/lastYearReviews";
        String loyalCustomersPath = "src/test/resources/output/loyalCustomers";
        String categoryStatsPath = "src/test/resources/output/categoryStats";
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        productsAndScores = DataHandler.readRDD(sparkSession, productsScoreInputPath)
                .map(ReviewConverter::rowToProductsAndScore);

        lastYearReviews = DataHandler.readRDD(sparkSession, lastYearReviewsPath)
                .map(ReviewConverter::rowToEntity);

        loyalCustomers = DataHandler.readRDD(sparkSession, loyalCustomersPath)
                .map(ReviewConverter::rowToCustomerStats);

        categoryStats = DataHandler.readRDD(sparkSession, categoryStatsPath)
                .map(ReviewConverter::rowToCategoryStats);

    }

    @Test
    public void testDataIntegrityForProductsAndScore() {
        productsAndScores.foreach(productAndScore -> {
            String score = productAndScore._2;
            assertTrue(Double.parseDouble(score) < 5.1);
        });
    }

    @Test
    public void testDataIntegrityForLastYearReviews() {
        lastYearReviews.foreach(review -> {
            LocalDate reviewDate = LocalDate.parse(review.review_date);
            assertTrue(reviewDate.compareTo(LocalDate.of(2014, 12, 12).minusYears(1)) > 0);
        });
    }

    @Test
    public void testCategoryStats(){
        categoryStats.foreach(categoryStat ->
                assertTrue(categoryStat._2 >= 0)
        );
    }

    @Test
    public void testCustomers(){
        loyalCustomers.foreach(customerStat ->
                assertTrue(customerStat._2 >= 0)
        );
    }

}
