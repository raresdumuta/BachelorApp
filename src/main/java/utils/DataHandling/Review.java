package utils.DataHandling;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

@Slf4j
public class Review implements Serializable {
    public final String marketplace;
    public final String customer_id;
    public final String product_id;
    public final String review_id;
    public final String product_parent;
    public final String product_title;
    public final String product_category;
    public final String star_rating;
    public final String helpful_votes;
    public final String total_votes;
    public final String vine;
    public final String verified_purchase;
    public final String review_headline;
    public final String review_body;
    public final String review_date;

    public Review(String marketPlace, String customer_id,
                  String product_id, String review_id,
                  String product_parent, String product_title,
                  String product_category, String star_rating,
                  String helpful_votes, String total_votes,
                  String vine, String verified_purchase,
                  String review_headline, String review_body, String review_date){
        this.marketplace = marketPlace;
        this.customer_id = customer_id;
        this.product_id = product_id;

        this.review_id = review_id;
        this.product_parent = product_parent;
        this.product_title = product_title;
        this.product_category = product_category;
        this.star_rating = star_rating;
        this.helpful_votes = helpful_votes;
        this.total_votes = total_votes;
        this.vine = vine;
        this.verified_purchase = verified_purchase;
        this.review_headline = review_headline;
        this.review_body = review_body;
        this.review_date = review_date;
    }
}
