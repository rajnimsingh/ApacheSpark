package com.spark.definitiveGuide.structured.operations;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CountRows {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();

        Dataset<Row> originalDataset = sparkSession.read().option("header", "true")
                .option("inferSchema", "false")
                .csv("src/main/resources/definitiveGuide/retail-data/by-day/2010-12-01.csv");

        System.out.println("Total Rows : "+originalDataset.count());
        System.out.println("Unique Rows : "+originalDataset.distinct().count());

        // select Random records
        final Dataset<Row> randomDataset = originalDataset.sample(false, 0.5, 5);
        System.out.println(randomDataset.count());
        sparkSession.close();
    }
}
