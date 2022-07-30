package com.spark.definitiveGuide.aggregations;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class MinAndMax {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();

        final Dataset<Row> dataset = sparkSession.read().option("header", "true")
                .option("inferSchema", "false")
                .csv("src/main/resources/definitiveGuide/retail-data/all/*.csv");

        System.out.println("Minimum and Maximum Quantity is as below:");
        final Dataset<Row> minMaxQuantity = dataset.select(functions.max(functions.col("Quantity")).as("MAXIMUM_QUANTITY"),
                functions.min(functions.col("Quantity")).as("MINIMUM_QUANTITY"));
        minMaxQuantity.show();

        System.out.println("Sum is as below:");
        final Dataset<Row> quantityDataset = dataset.select(functions.sum(functions.col("Quantity")));
        quantityDataset.show();

        System.out.println("Distinct Sum is as below:");
        final Dataset<Row> distinctQtySum = dataset.select(functions.sum_distinct(functions.col("Quantity")));
        distinctQtySum.show();

        sparkSession.close();
    }
}
