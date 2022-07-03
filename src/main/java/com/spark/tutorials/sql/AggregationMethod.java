package com.spark.tutorials.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

public class AggregationMethod {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");

        // we can also use inferSchema but it is very expensive
        Dataset<Row> newDataset = dataset.select(col("subject"), col("score").cast(DataTypes.IntegerType),
                col("student_id").cast(DataTypes.IntegerType).alias("student_id"));
        newDataset = newDataset.groupBy(col("subject")).max("student_id");

        newDataset.show();

        Dataset<Row> aggDataset = dataset.groupBy(col("subject"))
                .agg(
                        max(col("score")).alias("Max Score"),
                        min("score").alias("Min Score")
                );

        aggDataset = aggDataset.orderBy("subject");
        aggDataset.show();

        buildAggregationsUsingPivot(dataset);
    }

    static void buildAggregationsUsingPivot(Dataset<Row> originalDataset) {
        originalDataset = originalDataset.groupBy(col("subject"))
                .pivot(col("year"))
                .agg(
                        round(avg(col("score")), 2).alias("SCORE_AVG"),
                        round(stddev(col("score")), 2).alias("SCORE_STDDEV")
                );

        originalDataset.show();
    }
}
