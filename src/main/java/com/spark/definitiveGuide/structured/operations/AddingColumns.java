package com.spark.definitiveGuide.structured.operations;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class AddingColumns {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();

        // we can also make Spark session to be configured with spark.sql.caseSensitive as true
        System.out.println("Adding a column using Spark functions");
        Dataset<Row> dataset = sparkSession.read().option("header", "true").option("inferSchema", "false")
                        .csv("src/main/resources/definitiveGuide/2015-summary.csv");

        dataset = dataset.withColumn("Literal Value", functions.lit("1"));
        dataset = dataset.withColumn("withinCountry", functions.expr("DEST_COUNTRY = ORIGIN_COUNTRY_NAME"));
        dataset.show();

        System.out.println("Renaming columns");

        dataset = dataset.withColumnRenamed("DEST_COUNTRY", "destination");
        dataset.show();

        System.out.println("Adding a long name");
        dataset = dataset.withColumn("This Long Column-name let us see if this works or fails", functions.col("destination"));
        dataset.show();

        System.out.println("Select a long name using expression");
        dataset = dataset.selectExpr("`This Long Column-name let us see if this works or fails`");
        dataset.show();
        sparkSession.close();
    }
}
