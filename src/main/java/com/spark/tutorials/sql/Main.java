package com.spark.tutorials.sql;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static SparkSession buildSparkSession() {
        System.setProperty("hadoop.home.dir", "/Users/rajniubhi");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder()
                .appName("testingSQL")
                .master("local[*]")
             //   .config("spark.sql.caseSensitive", true)
                .getOrCreate();

        System.out.println("Spark Session built successfully.");
        return sparkSession;
    }
    public static void main(String[] args) {

        SparkSession sparkSession = buildSparkSession();

        Dataset<Row> dataset = sparkSession.read().option("header","true").csv("src/main/resources/orders/orders.csv");

        dataset.show();

        // get the first row from dataset
        Row row = dataset.first();

        // get the user_id, item_name and item_category.
        String userId = row.getAs("user_id");
        String itemName = row.getAs("item_name");
        String itemCategory = row.getAs("item_category");
        System.out.println("User Id: " + userId);
        System.out.println("Item Name: " + itemName);
        System.out.println("Category : " + itemCategory);

        sparkSession.close();
    }
}
