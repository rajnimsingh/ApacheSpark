package com.spark.definitiveGuide.structured.operations;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SelectExpressions {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();
        String destinationCountryName = "Destination_country_name";
        String originCountryName = "Origin_country_name";
        String count = "Count";

        final Dataset<Row> dataset = sparkSession.read().option("header", "true").option("inferSchema", "false")
                .schema(new StructType(
                        new StructField[] {
                                new StructField(destinationCountryName, DataTypes.StringType, false, Metadata.empty()),
                                new StructField(originCountryName, DataTypes.StringType, false, Metadata.empty()),
                                new StructField(count, DataTypes.IntegerType, false, Metadata.empty())
                        }
                ))
                .csv("src/main/resources/definitiveGuide/2015-summary.csv");
        System.out.println("The entire dataset is as follow:");
        dataset.show();

        System.out.println("Selecting only Destination country name and origin country name");
        dataset.select(functions.col(destinationCountryName), functions.col(originCountryName)).show();

        System.out.println("Another alternative way to select data");
        dataset.select(dataset.col(destinationCountryName), functions.column(originCountryName), functions.expr(count)).show();

        System.out.println("Using alias in Select clause is as below:");
        dataset.select(functions.expr(destinationCountryName + " AS DEST_CNT_NAME")).show();

        System.out.println("Another example of Using EXPR");
        dataset.selectExpr(destinationCountryName +" AS destination", originCountryName).show();

        System.out.println("Adding extra columns using Expression");
        dataset.selectExpr("*", "(Destination_country_name = Origin_country_name) AS withinCountry").show();

        System.out.println("Average using expression");
        dataset.selectExpr("avg(count)", "count(distinct(Destination_country_name))").show();


        System.out.println("Adding literal values using Expressions");
        dataset.select(functions.col(destinationCountryName), functions.col(originCountryName), functions.col(count), functions.lit("23-Jul-22:12:20 AM").as("Timestamp")).show();
        sparkSession.close();
    }
}
