package com.spark.definitiveGuide.aggregations;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CustomerOrderTotalUDAF extends UserDefinedAggregateFunction {

    @Override
    public StructType inputSchema() {
        return DataTypes.createStructType(
                new StructField[] {
                        DataTypes.createStructField("CustomerID", DataTypes.StringType, false)
                }
        );
    }

    @Override
    public StructType bufferSchema() {
        return DataTypes.createStructType(
                new StructField[] {
                        DataTypes.createStructField("CustomerID", DataTypes.StringType, false)
                }
        );
    }

    @Override
    public DataType dataType() {
        return DataTypes.IntegerType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, null);
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        if(buffer.isNullAt(0)) {
            buffer.update(0, input.getInt(0));
        } else {
            buffer.update(0, input.getInt(0) + buffer.getInt(0));
        }
    }

    @Override
    public void merge(MutableAggregationBuffer buffer, Row row) {
        if (buffer.isNullAt(0)) {
            buffer.update(0, row.getInt(0));
        } else {
            buffer.update(0, row.getInt(0) + buffer.getInt(0));
        }
    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getInt(0);
    }
}
