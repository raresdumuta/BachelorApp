package utils.DataHandling;

import java.io.Serializable;

public class SumAndCount implements Serializable {

    public Long count;
    public Long sum;

    public SumAndCount(Long sum , Long count){
        this.sum = sum;
        this.count = count;
    }
}
