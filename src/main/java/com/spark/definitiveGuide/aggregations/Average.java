package com.spark.definitiveGuide.aggregations;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Average {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();

        final Dataset<Row> dataset = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "false")
                .csv("src/main/resources/definitiveGuide/retail-data/all/*.csv");

        dataset.select(sum(col("Quantity")).alias("Total_Purchases"),
                        count(col("Quantity")).alias("Total_Transactions"),
                        avg(col("Quantity")).alias("Average_Purchases"),
                        expr("mean(Quantity)").as("Mean_Purchases"))
                .selectExpr("Total_Purchases/Total_Transactions", "Average_Purchases", "Mean_Purchases")
                .show();


        sparkSession.close();
    }
}
