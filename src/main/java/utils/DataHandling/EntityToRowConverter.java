package utils.DataHandling;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.function.Function;

public class EntityToRowConverter implements Function<Review, Row>, Serializable {
    @Override
    public Row apply(Review review) {
        return null;
    }
}
