package com.spark.definitiveGuide;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

public class WindowFunctions {

    public static void main(String[] args) {
        // get the order data
        Dataset<Row> orderData = ReadOrderData.getOrderData();

        // find max count items which were sold in 2017
        WindowSpec windowSpec = Window.partitionBy(functions.col("item_category"))
                .orderBy(functions.desc("price"));

        orderData.selectExpr("item_name", "price", "item_category")
                .groupBy(
                        functions.col("item_name"), functions.col("item_category")
                )
                .max("price")
                .orderBy(functions.desc("max(price)"))
                .show();

        // using window functions
        orderData.select(functions.col("item_name"), functions.col("item_category")
                , functions.col("price")
                , functions.row_number().over(windowSpec)).show();
    }
}
