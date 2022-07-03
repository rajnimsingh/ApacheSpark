package com.spark.definitiveGuide;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GeneratingRange {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();

        Dataset<Row> dataset = sparkSession.range(1000).toDF("number");
        dataset.show(5);

        // find all the even numbers
        System.out.println("Finding dataframe of all the even numbers");
        dataset = dataset.where("number % 2 = 0");
        dataset.show(5);

        System.out.print("Total number of the even numbers in dataframe : ");
        System.out.println(dataset.count());
    }
}
