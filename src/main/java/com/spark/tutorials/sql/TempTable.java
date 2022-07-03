package com.spark.tutorials.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TempTable {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();

        // we can create temp table views from dataset in spark sql
        Dataset<Row> rowDataset = sparkSession.read().option("header","true").csv("src/main/resources/orders/orders.csv");
        rowDataset.createOrReplaceTempView("orders_temp_view");
        rowDataset = sparkSession.sql("select invoice_id, line_item_id from orders_temp_view");
        rowDataset.show();

        sparkSession.close();
    }
}
