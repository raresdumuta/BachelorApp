package main;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Serializable;
import scala.Tuple2;
import utils.DataHandling.Review;

import java.time.LocalDate;

public class EnhancementApplication implements Serializable {
    private final JavaRDD<Review> data;

    public EnhancementApplication(JavaRDD<Review> inputData) {
        data = inputData;
    }

    /**
     * @return the reviews no older than 1 year
     */
    public JavaRDD<Review> lastYearReviews() {
        return data.filter(review -> review.review_date != null)
                .filter(review -> convertToLocalDate(review.review_date).
                                compareTo(LocalDate.of(2014,12,12).minusYears(1)) > 0);
    }

    /**
     * this method convert from date as a string in a JavaCore LocalDate class for a better management of the date
     */
    public LocalDate convertToLocalDate(String dateString) {
        try{
            String[] dateSplit = dateString.split("-");
            int year = Integer.parseInt(dateSplit[0]);
            int month = Integer.parseInt(dateSplit[1]);
            int daysOfTheMonth = Integer.parseInt(dateSplit[2]);
            return LocalDate.of(year, month, daysOfTheMonth);
        } catch (NullPointerException npe){
            System.out.println(dateString);
            return LocalDate.MIN;
        }
    }

    /**
     * @return a map containing each category a with the total number of reviews for that category
     */
    public JavaPairRDD<String, Long> getCategoryStats() {
        return data.mapToPair(review -> new Tuple2<>(review.product_category,1L)).reduceByKey(Long::sum);

    }

    /**
     * @return the total number of reviews analyzed
     */
    public Long getTotalNumberOfReviews() {
        return data.count();
    }

    /**
     * @return each customer will have the number of items bought by each customer
     */
    public JavaPairRDD<String, Long> itemsBoughtByCustomers(){
        JavaPairRDD<String, Long> mappedData = data.mapToPair(review -> new Tuple2<>(review.customer_id, 1L));
        return mappedData.reduceByKey(Long::sum);
    }

    public JavaPairRDD<String,Long> getLoyalCustomers(Long loyaltyLevel){
        return itemsBoughtByCustomers().filter(customer-> customer._2() > loyaltyLevel);
    }

}
