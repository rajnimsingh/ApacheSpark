package com.spark.definitiveGuide.aggregations;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class FirstAndLast {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();
        final Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/definitiveGuide/retail-data/all/*.csv");
        dataset.createOrReplaceTempView("retail_data");

        sparkSession.sql("select first(StockCode), last(StockCode) from retail_data").show();
        final Dataset<Row> firstLastDataset = dataset.select(functions.first(functions.col("StockCode")), functions.last(functions.col("StockCode")));
        firstLastDataset.show();

        firstLastDataset.explain();
        sparkSession.close();
    }
}
