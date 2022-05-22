package main;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Serializable;
import scala.Tuple2;
import utils.DataHandling.SumAndCount;
import utils.DataHandling.Review;

import java.text.DecimalFormat;
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
                        compareTo(LocalDate.of(2014, 12, 12).minusYears(1)) > 0);
    }

    /**
     * this method convert from date as a string in a JavaCore LocalDate class for a better management of the date
     */
    private LocalDate convertToLocalDate(String dateString) {
        try {
            String[] dateSplit = dateString.split("-");
            int year = Integer.parseInt(dateSplit[0]);
            int month = Integer.parseInt(dateSplit[1]);
            int daysOfTheMonth = Integer.parseInt(dateSplit[2]);
            return LocalDate.of(year, month, daysOfTheMonth);
        } catch (NullPointerException npe) {
            System.out.println(dateString);
            return LocalDate.MIN;
        }
    }

    /**
     * @return a map containing each category a with the total number of reviews for that category
     */
    public JavaPairRDD<String, Long> getCategoryStats() {
        return data.mapToPair(review -> new Tuple2<>(review.product_category, 1L)).reduceByKey(Long::sum);

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
    public JavaPairRDD<String, Long> itemsBoughtByCustomers() {
        JavaPairRDD<String, Long> mappedData = data.mapToPair(review -> new Tuple2<>(review.customer_id, 1L));
        return mappedData.reduceByKey(Long::sum);
    }

    public JavaPairRDD<String, Long> getLoyalCustomers(Long loyaltyLevel) {
        return itemsBoughtByCustomers().filter(customer -> customer._2() > loyaltyLevel);
    }

    public JavaPairRDD<String, String> calculateAverageScoreForProducts() {
        JavaPairRDD<String, Long> productAndRating = mapToProductAndStar(data);

        Function<Long, SumAndCount> combiner = (Long x) -> new SumAndCount(x, 1L);

        Function2<SumAndCount, Long, SumAndCount> mergeValues = (SumAndCount a, Long x) -> {
            a.sum += x;
            a.count += 1L;
            return a;
        };

        Function2<SumAndCount, SumAndCount, SumAndCount> merger = (SumAndCount a, SumAndCount b) -> {
            a.sum += b.sum;
            a.count += b.count;
            return a;
        };
        DecimalFormat df = new DecimalFormat("#.#");

        return productAndRating.combineByKey(combiner, mergeValues, merger)
                .mapToPair(value -> new Tuple2<>(value._1, df.format(value._2().sum * 1.0/value._2.count)));
    }

    private JavaPairRDD<String, Long> mapToProductAndStar(JavaRDD<Review> reviews) {
        return reviews.mapToPair(review -> new Tuple2<>(review.product_id, Long.parseLong(review.star_rating)));
    }

}
