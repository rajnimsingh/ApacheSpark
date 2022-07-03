package com.spark.tutorials.sql;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SparkFilters {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();

        // Apply the filters on order data for the item digital apparatus.
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/orders/orders.csv");

        // fetch only the digital apparatus items
        filteringUsingSql(dataset);
        filteringUsingLambda(dataset);
        filteringUsingColumns(dataset);
        filteringUsingColumnsFunction(dataset);
        sparkSession.close();
    }

    private static void filteringUsingSql(Dataset<Row> dataset) {
        Dataset<Row> filteredDataSet = dataset.filter("item_name = 'digital apparatus' ");
        filteredDataSet.show();
    }

    private static void filteringUsingLambda(Dataset<Row> dataset) {
        Dataset<Row> filteredDataSet = dataset.filter((FilterFunction<Row>) d -> d.getAs("item_name").equals("digital apparatus"));
        filteredDataSet.show();
    }

    private static void filteringUsingColumns(Dataset<Row> dataset) {
        Column itemColumn = dataset.col("item_name");

        Dataset<Row> filteredDataSet = dataset.filter(itemColumn.equalTo("digital apparatus"));
        filteredDataSet.show();
    }

    private static void filteringUsingColumnsFunction(Dataset<Row> dataset) {
        Dataset<Row> filteredDataSet = dataset.filter(col("item_name").equalTo("digital apparatus"));
        filteredDataSet.show();
    }
}
