package utils.DataHandling;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;

public class ReviewConverter {
    public static Review rowToEntity(Row row) {
        try {
            String marketPlace = row.getAs("marketplace");
            String customer_id = row.getAs("customer_id").toString();
            String product_id = row.getAs("product_id").toString();
            String review_id = row.getAs("review_id").toString() ;
            String product_parent = row.getAs("product_parent").toString();
            String product_title = row.getAs("product_title").toString();
            String product_category = row.getAs("product_category").toString();
            String star_rating = row.getAs("star_rating").toString() ;
            String helpful_votes = row.getAs("helpful_votes").toString();
            String total_votes = row.getAs("total_votes").toString();
            String vine = row.getAs("vine").toString();
            String verified_purchase = row.getAs("verified_purchase").toString();
            String review_headline = row.getAs("review_headline").toString();
            String review_body = row.getAs("review_body");
            String review_date = row.getAs("review_date");
            return new Review(marketPlace, customer_id, product_id, review_id, product_parent,
                    product_title, product_category, star_rating, helpful_votes,
                    total_votes, vine, verified_purchase, review_headline, review_body, review_date);
        }catch (NullPointerException npe){
            return null;
        }

    }

    public static Row entityToRow(Review review) {
        return RowFactory.create(review.marketplace,review.customer_id,review.product_id,review.review_id,review.product_parent,
                review.product_title, review.product_category,review.star_rating,review.helpful_votes,review.total_votes
                ,review.vine,review.verified_purchase,review.review_headline,
                review.review_body,review.review_date);
    }

    public static Row stringLongToRow(Tuple2<String, Long> customer){
        return RowFactory.create(customer._1(),customer._2());
    }

    public static Row stringStringToRow(Tuple2<String,String> entry){
        return RowFactory.create(entry._1(),entry._2());
    }
}
