package utils.DataHandling;

import org.apache.spark.api.java.JavaRDD;

public interface RddWriterInterface {
    void write(JavaRDD<Review> var1);
}
