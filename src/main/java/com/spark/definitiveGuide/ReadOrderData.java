package com.spark.definitiveGuide;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class ReadOrderData {
    static SparkSession sparkSession = Main.buildSparkSession();

    public static void main(String[] args) {
        // read the order data from a CSV
        final Dataset<Row> orders = getOrderData();
        Dataset<Row> orderDataSet = getOrderData();

        // convert the dataframe to a temp table for query.
        orderDataSet.createOrReplaceTempView("order_data");

        // sort the data and see plan of execution by Spark
        orderDataSet.sort("created_at").explain();

        System.out.println("Below is the execution plan for SQL");
        Dataset<Row> ordersData = sparkSession.sql("select item_name, count(1) from order_data group by item_name");
        ordersData.explain();

        System.out.println("Below is the execution plan for Java API");
        orderDataSet = orderDataSet.groupBy("item_name").count();
        orderDataSet.explain();

        findMaxPrice(orders);
        findTop5MostSellingItems(orders);

        sparkSession.close();
    }

    static Dataset<Row> getOrderData() {
        return sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("src/main/resources/orders/orders.csv");
    }

    static void findMaxPrice(Dataset<Row> orders) {
        System.out.println("Check Maximum price by SQL");
        final Dataset<Row> rowDataset = sparkSession.sql("SELECT max(price) from order_data");
        rowDataset.show();

        System.out.println("Check maximum price by using JAVA API");
        final Dataset<Row> priceDataset = orders.select(functions.max(functions.col("price")));
        priceDataset.show();
    }

    static void findTop5MostSellingItems(Dataset<Row> orders) {
        sparkSession.sql("select item_name, count(1) as item_count " +
                "from order_data " +
                "group by item_name " +
                "order by item_count desc " +
                "limit 5").show();

        // below is the same version of above code using Java API.
        orders.groupBy(orders.col("item_name")).count()
                .withColumnRenamed("count", "item_count")
                .sort(functions.desc("item_count"))
                .limit(5).show();
    }
}
