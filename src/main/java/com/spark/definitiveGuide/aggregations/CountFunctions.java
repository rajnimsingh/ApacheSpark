package com.spark.definitiveGuide.aggregations;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class CountFunctions {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();
        final Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/definitiveGuide/retail-data/all/*.csv");

        count(sparkSession, dataset);
        distinctCount(sparkSession, dataset);
        approxDistinctCount(sparkSession, dataset);

        sparkSession.close();
    }

    static void count(SparkSession sparkSession, Dataset<Row> dataset) {

        System.out.println("Total Count using function is : "+dataset.count());

        // Now use the sql query
        dataset.createOrReplaceTempView("retail_data");
        final Dataset<Row> countQueryDataset = sparkSession.sql("select count(*) from retail_data");

        // use sql methods to select count of a column in dataset
        final Dataset<Row> stockCodeDataset = dataset.select(functions.count("StockCode"));
        stockCodeDataset.show();

        // using selectExpr
        final Dataset<Row> selectExprDataset = dataset.selectExpr("count(StockCode)");
        selectExprDataset.show();

        countQueryDataset.show();
    }

    static void distinctCount(SparkSession sparkSession, Dataset<Row> dataset) {
        System.out.println("Distinct Count method starts below:");
        System.out.println("Total Count using function is : "+dataset.count());

        // Now use the sql query
        dataset.createOrReplaceTempView("retail_data");
        final Dataset<Row> countQueryDataset = sparkSession.sql("select count(distinct(*)) from retail_data");
        countQueryDataset.show();

        // use sql methods to select count of a column in dataset
        final Dataset<Row> stockCodeDataset = dataset.select(functions.countDistinct("StockCode"));
        stockCodeDataset.show();

        // using selectExpr
        final Dataset<Row> selectExprDataset = dataset.selectExpr("count( distinct StockCode ) as cnt");
        selectExprDataset.show();
    }

    static void approxDistinctCount(SparkSession sparkSession, Dataset<Row> dataset) {
        System.out.println();
        System.out.println("ApproxDistinct Count starts below::::::");
        final Dataset<Row> stockCodeApproxCount = dataset.select(functions.approx_count_distinct(functions.col("StockCode")));
        stockCodeApproxCount.show();

        final Dataset<Row> stockCodeRatioCount = dataset.select(functions.approx_count_distinct("StockCode", 0.1));
        stockCodeRatioCount.show();

        final Dataset<Row> retailDataset = sparkSession.sql("select approx_count_distinct(StockCode) from retail_data");
        retailDataset.show();

        final Dataset<Row> retailWithRatioDataset = sparkSession.sql("select approx_count_distinct(StockCode, 0.1) from retail_data");
        retailWithRatioDataset.show();
    }
}
