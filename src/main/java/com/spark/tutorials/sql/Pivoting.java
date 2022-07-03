package com.spark.tutorials.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

public class Pivoting {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();
        Dataset<Row> rowDataset = sparkSession.read().option("header", true).csv("src/main/resources/logs/biglog.txt");
        rowDataset = rowDataset.select(functions.col("level"), functions.date_format(functions.col("datetime"), "MMMM").alias("month"),
                functions.date_format(functions.col("datetime"), "yyyy").alias("Year"));

        List<Object> monthList = Arrays.asList("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December");

        Dataset<Row> dataset = rowDataset.groupBy(functions.col("level")).pivot(functions.col("month"), monthList).count();
        dataset = dataset.orderBy(functions.col("level"));
        dataset.show();

        // Drill-down further to check huge spike for November
        Dataset<Row> spikedData = rowDataset.filter(functions.col("month").equalTo("November")).filter(functions.col("level").equalTo("FATAL"));
        spikedData = spikedData.groupBy(functions.col("level")).pivot(functions.col("Year")).count();
        spikedData.show();

    }
}
