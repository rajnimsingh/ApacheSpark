package com.spark.definitiveGuide.structured.operations;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class AppendingDatasets {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();
        Dataset<Row> originalDataset = sparkSession.read().option("header", "true").csv("src/main/resources/definitiveGuide/2015-summary.csv");
        System.out.println("Original Dataset Count : "+originalDataset.count());
        StructType schema = new StructType(
                new StructField[] {
                        new StructField("DESTINATION", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("ORIGIN_COUNTRY_NAME", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("COUNT", DataTypes.IntegerType, false, Metadata.empty())
                }
        );

        Dataset<Row> secondDataset = sparkSession.createDataFrame(List.of(
                RowFactory.create("Canada", "United States", 10),
                RowFactory.create("Canada", "Russia", 34),
                RowFactory.create("Canada", "India", 7878),
                RowFactory.create("Canada", "New Zeeland", 78),
                RowFactory.create("Canada", "United Kingdom", 22)
        ), schema);
        System.out.println("Second dataset Count: "+secondDataset.count());

        final Dataset<Row> unionDataset = originalDataset.union(secondDataset);

        System.out.println("Total Count after Union : "+unionDataset.count());
        unionDataset.show();

        // sorting the data in a dataframe
        unionDataset.selectExpr("*", "cast(Count as int) as COUNT_VAL").orderBy(functions.col("COUNT_VAL").desc(), functions.col("DEST_COUNTRY").asc()).show();

        Dataset<Row> sortedDataset = unionDataset.orderBy(functions.col("Count").desc(), functions.col("DEST_COUNTRY").asc() );

        System.out.println("Sorted Data set is as below:");
        sortedDataset.show();

        sortedDataset.explain();

        // sorting using partitions
        Dataset<Row> sortedDatasetWithPartitions = unionDataset.sortWithinPartitions(functions.col("Count").desc());
        sortedDatasetWithPartitions.show();
        sparkSession.close();
    }
}
