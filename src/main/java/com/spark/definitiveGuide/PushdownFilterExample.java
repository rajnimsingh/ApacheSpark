package com.spark.definitiveGuide;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.concurrent.TimeUnit;

public class PushdownFilterExample {
    public static void main(String[] args) throws InterruptedException {
        SparkSession sparkSession = Main.buildSparkSession();

        final Dataset<Row> csvDataset = sparkSession.read()
                .option("header", "true")
                .option("delimiter", "|")
                .csv("src/main/resources/data.csv");

        // This is not a push down see the output of plan
        csvDataset.filter(functions.col("age").gt(25)).explain();

        // This is a push down see the output of plan - data gets filtered out before loading into memory
        csvDataset.filter(functions.col("position").equalTo("tester")).explain();

        long count = csvDataset.filter(functions.col("position").equalTo("tester")).count();
        System.out.println("Count is "+count);

        sparkSession.close();
    }
}
