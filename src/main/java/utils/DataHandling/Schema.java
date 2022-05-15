package utils.DataHandling;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
//
//public class Schema {
//    public Schema() {
//    }
//    public StructType asStructType() {
//
//        return new StructType()
//                .add(new Column("marketplace",DataTypes.StringType,true))
//        .add("customer_id",DataTypes.StringType,true)
//        .add("product_id",DataTypes.StringType,true)
//        .add("review_id",DataTypes.StringType,true)
//        .add("product_parent",DataTypes.StringType,true)
//        .add("product_title",DataTypes.StringType,true)
//        .add("product_category",DataTypes.StringType,true)
//        .add("star_rating",DataTypes.StringType,true)
//        .add("helpful_votes",DataTypes.StringType,true)
//        .add("total_votes",DataTypes.StringType,true)
//        .add("vine",DataTypes.StringType,true)
//        .add("verified_purchase",DataTypes.StringType,true)
//        .add("review_headline",DataTypes.StringType,true)
//        .add("review_body",DataTypes.StringType,true)
//        .add("review_date",DataTypes.StringType,true);
//    }
//
//}

public abstract class Schema {
    public Schema() {
    }

    public StructType asStructType() {
        List<StructField> fields = this.columns().map((column) -> {
            return DataTypes.createStructField(column.name(), column.type(), column.isNullable());
        }).collect(Collectors.toList());
        return DataTypes.createStructType(fields);
    }

    protected abstract Stream<Column> columns();
}
