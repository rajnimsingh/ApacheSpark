package com.spark.definitiveGuide.aggregations;

import com.google.common.collect.Maps;
import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import javax.xml.crypto.Data;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class Grouping {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();

        final Dataset<Row> dataset = sparkSession.read().option("header", "true").option("inferSchema", "false")
                .csv("src/main/resources/definitiveGuide/retail-data/all/*.csv");
        dataset.groupBy(col("InvoiceNo"), col("CustomerID")).count().show();

        // Total Quantity order by customer
        dataset.groupBy(col("CustomerID")).agg(sum(col("Quantity")).alias("Total Qty")).orderBy(col("Total Qty").desc()).show();

        // Max Quantity Order by a Customer
        dataset.groupBy(col("CustomerID")).agg(max(col("Quantity")).cast(DataTypes.DoubleType).alias("Quantity")).orderBy(col("Quantity").desc()).show();

        // total Qty order
        dataset.agg(sum(col("Quantity")).cast(DataTypes.DoubleType).alias("Total Qty")).orderBy(col("Total Qty").desc_nulls_last()).show();

        dataset.createOrReplaceTempView("retail_data");

        sparkSession.sql("select CustomerID, sum(Quantity) from retail_data where CustomerID in (15555, 12346) group by CustomerID").show();

        // Grouping with Maps
        Map<String, String> groupMap = Maps.newHashMap();
        groupMap.put("Quantity", "avg");

        dataset.agg(groupMap).show();

        sparkSession.close();
    }
}
