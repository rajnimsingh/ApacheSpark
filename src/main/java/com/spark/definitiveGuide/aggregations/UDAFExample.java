package com.spark.definitiveGuide.aggregations;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class UDAFExample {
    public static void main(String[] args) {
        SparkSession sparkContext = Main.buildSparkSession();
        Dataset<Row> dataset = sparkContext.read().option("header", "true").csv("src/main/resources/definitiveGuide/retail-data/all/online-retail-dataset.csv");
        sparkContext.udf().register("sumOrder", new CustomerOrderTotalUDAF());
        dataset = dataset.withColumn("total", functions.call_udf("sumOrder", functions.col("CustomerID")));
        dataset.show();
        sparkContext.close();
    }
}
