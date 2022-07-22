package com.spark.definitiveGuide.structured.streaming;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.StructType;

public class RetailDatasetLoading {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();

        Dataset<Row> staticDataFrame = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                //.csv("src/main/resources/definitiveGuide/retail-data/by-day/*.csv");
                .csv("src/main/resources/definitiveGuide/retail-data/test.csv");
        staticDataFrame.createOrReplaceTempView("retail_data");
        final StructType schema = staticDataFrame.schema();
        System.out.println(schema);

        // Add a total cost variable.
        WindowSpec windowSpec = Window.partitionBy("InvoiceDate", "CustomerId").orderBy(("total_cost"));
        final Dataset<Row> dataset = staticDataFrame.selectExpr("CustomerId", "InvoiceDate", "(UnitPrice * Quantity) as total_cost").withColumn("RANK", functions.rank().over(windowSpec));
        dataset.show(25);

        sparkSession.close();
    }
}
