package com.spark.tutorials.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class InMemoryData {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();

        List<Row> inMemoryData = new ArrayList<>();

        inMemoryData.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemoryData.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemoryData.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemoryData.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemoryData.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));

        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };

        StructType schema = new StructType(fields);
        Dataset<Row> dataset = sparkSession.createDataFrame(inMemoryData, schema);
        System.out.println("Raw dataset is as below::");
        dataset.show();
        // grouping and aggregation

        dataset.createOrReplaceTempView("logging_table");

        Dataset<Row> rowDataset = sparkSession.sql("select level, count(datetime) from logging_table group by level order by level");
        System.out.println("Grouped/Aggregated dataset is as below:");
        rowDataset.show();

        System.out.println("Collect as list in grouping by");

        Dataset<Row> sqlResultSet = sparkSession.sql("select level, collect_list(datetime) from logging_table group by level order by level");
        sqlResultSet.show();

        System.out.println("Date Time formatting in Spark Sql as below:");
        final Dataset<Row> dateFormatDataset = sparkSession.sql("select level, date_format(datetime, 'MMM') as month from logging_table");
        dateFormatDataset.show();

        sparkSession.close();
    }
}
