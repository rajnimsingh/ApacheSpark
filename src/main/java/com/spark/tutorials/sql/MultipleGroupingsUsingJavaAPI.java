package com.spark.tutorials.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class MultipleGroupingsUsingJavaAPI {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();

        Dataset<Row> rowDataset = sparkSession.read().option("header", true).csv("src/main/resources/logs/biglog.txt");

       Dataset<Row> selectDataSet = rowDataset.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").cast(DataTypes.IntegerType).alias("monthNum"));
        selectDataSet = selectDataSet.groupBy(col("level"), col("month"), col("monthNum")).count();
        selectDataSet = selectDataSet.orderBy(col("monthNum"), col("level"));
        selectDataSet = selectDataSet.drop(col("monthNum"));
        selectDataSet.show();

        sparkSession.close();
    }
}
