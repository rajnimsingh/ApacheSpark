package com.spark.tutorials.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MultipleGroupings {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();


        Dataset<Row> logDataSet = sparkSession.read().option("header", true).csv("src/main/resources/logs/biglog.txt");

        logDataSet.createOrReplaceTempView("logging_table");

        logDataSet = sparkSession.sql("select level, date_format(datetime, 'MMM') as month, count(1) as total from logging_table group by level, month order by month");

        logDataSet.show();

        logDataSet.createOrReplaceTempView("results_table");

        final Dataset<Row> rowDataset = sparkSession.sql("select sum(total) from results_table");
        rowDataset.show();

        logDataSet = sparkSession.sql("select level, date_format(datetime, 'MMM') as month, first(date_format(datetime, 'M')) as monthnum, count(1) as total from logging_table " +
                "group by level, month order by monthnum");

        logDataSet.show();

        System.out.println();
        System.out.println("The better version of the above output is as below:");
        logDataSet = sparkSession.sql("select level, date_format(datetime, 'MMM') as month, count(1) as total from logging_table " +
                "group by level, month order by cast(first(date_format(datetime, 'M')) as int), level");

        logDataSet.show();
        sparkSession.close();
    }
}
