package com.spark.definitiveGuide;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

public class WindowFunctions {
    static SparkSession sparkSession = Main.buildSparkSession();

    public static void main(String[] args) {
        // get the order data
        Dataset<Row> orderData = ReadOrderData.getOrderData();

        // find max count items which were sold in 2017

        WindowSpec windowSpec = Window.partitionBy(functions.col("item_category"))
                .orderBy(functions.desc("price"));

        orderData.selectExpr("item_name", "price")
                .groupBy(
                        functions.col("item_name")
                )
                .max("price");
    }
}
