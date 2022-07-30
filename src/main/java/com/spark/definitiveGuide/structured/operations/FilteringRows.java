package com.spark.definitiveGuide.structured.operations;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class FilteringRows {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();

        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/definitiveGuide/retail-data/by-day/2010-12-01.csv");
        System.out.println("Total count before filtering: " + dataset.count());
        dataset = dataset.where(functions.col("customerId").equalTo(17850));
        System.out.println("Total count after filtering: " + dataset.count());
        dataset.explain();

        System.out.println("An alternative way to filter using filter method");
        Dataset<Row> newDataset = sparkSession.read().option("header", "true").csv("src/main/resources/definitiveGuide/retail-data/by-day/2010-12-01.csv");
        Dataset<Row> filteredDataset = newDataset.filter(functions.col("customerId") + " = 17850");
        System.out.println("Total count after filtering: " + filteredDataset.count());

        // Multiple filters can be chained together
        Dataset<Row> multipleFiltersDataset = newDataset.filter(functions.col("customerId").equalTo(17850))
                .filter(functions.col("Description").contains("HAND WARMER"));
        System.out.println("Count after two filters applied via chain: " + multipleFiltersDataset.count());
        multipleFiltersDataset.show();
        sparkSession.close();
    }
}
