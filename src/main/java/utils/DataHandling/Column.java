package utils.DataHandling;

import org.apache.spark.sql.types.DataType;

public class Column {
    private final String name;
    private final DataType type;
    private final boolean nullable;

    public Column(String name, DataType type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    public String name() {
        return this.name;
    }

    public DataType type() {
        return this.type;
    }

    public boolean isNullable() {
        return this.nullable;
    }
}
