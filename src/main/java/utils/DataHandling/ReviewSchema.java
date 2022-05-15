package utils.DataHandling;

import org.apache.spark.sql.types.DataTypes;

import java.util.stream.Stream;

public class ReviewSchema extends Schema{
       public static final Column marketplace;
       public static final Column customer_id ;
       public static final Column product_id;
       public static final Column review_id;
        public static final Column product_parent;
        public static final Column product_title;
        public static final Column product_category;
        public static final Column star_rating;
        public static final Column helpful_votes;
        public static final Column total_votes;
        public static final Column vine;
        public static final Column verified_purchase;
        public static final Column review_headline;
        public static final Column review_body;
        public static final Column review_date;
    protected Stream<Column> columns() {
        return Stream.of(marketplace,customer_id,product_id,review_id,product_parent,product_title,
                product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,
                review_body,review_date);
    }

    static
    {
        marketplace = new Column("marketplace", DataTypes.StringType,true);
        customer_id = new Column("customer_id", DataTypes.StringType,true);
        product_id = new Column("product_id", DataTypes.StringType,true);
        review_id = new Column("review_id", DataTypes.StringType,true);
        product_parent = new Column("product_parent", DataTypes.StringType,true);
        product_title = new Column("product_title", DataTypes.StringType,true);
        product_category = new Column("product_category", DataTypes.StringType,true);
        star_rating = new Column("star_rating", DataTypes.StringType,true);
        helpful_votes = new Column("helpful_votes", DataTypes.StringType,true);
        total_votes = new Column("total_votes", DataTypes.StringType,true);
        vine = new Column("vine", DataTypes.StringType,true);
        verified_purchase = new Column("verified_purchase", DataTypes.StringType,true);
        review_headline =  new Column("review_headline", DataTypes.StringType,true);
        review_body = new Column("review_body", DataTypes.StringType,true);
        review_date = new Column("review_date", DataTypes.StringType,true);
    }
}
