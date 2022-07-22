package com.spark.definitiveGuide.structured.operations;

import com.google.common.collect.Maps;
import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConverters$;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SchemaLoading {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();

        StructField[] structFields = new StructField[3];

        // DEST_COUNTRY,ORIGIN_COUNTRY_NAME,COUNT
        structFields[0] = new StructField("DEST_COUNTRY", DataTypes.StringType, false, Metadata.empty());
        structFields[1] = new StructField("ORIGIN_COUNTRY_NAME", DataTypes.StringType, false, Metadata.empty());
        structFields[2] = new StructField("COUNT", DataTypes.StringType, false, Metadata.empty());

        StructType structType = new StructType(structFields);

        Dataset<Row> csvDataSet = sparkSession.read().format("csv").option("header", "true").schema(structType)
                .load("src/main/resources/definitiveGuide/retail-data/test.csv");
        csvDataSet.show();
        csvDataSet.printSchema();

        simpleSchema(sparkSession);
        nestedSchema(sparkSession);
        Dataset<Row> dataFrame = usingArrayAndMapSchema(sparkSession);
        flattenMap(dataFrame);
        sparkSession.close();
    }

    public static void simpleSchema(SparkSession sparkSession) {
        StructType structType = new StructType(
                new StructField[]{
                        new StructField("FirstName", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("MiddleName", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("LastName", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("ID", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("Gender", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("Salary", DataTypes.IntegerType, false, Metadata.empty())
                }
        );
        List<Row> rows = List.of(
                RowFactory.create("James", "", "Smith", "36636", "M", 3000),
                RowFactory.create("Michael", "Rose", "", "40288", "M", 4000),
                RowFactory.create("Robert", "", "Williams", "42114", "M", 4500),
                RowFactory.create("Maria", "Anne", "James", "39192", "F", 3000),
                RowFactory.create("Jen", "Mary", "Brown", "65654", "M", 4000)
        );

        final Dataset<Row> dataFrame = sparkSession.createDataFrame(rows, structType);
        dataFrame.printSchema();

        dataFrame.show();
    }

    public static void nestedSchema(SparkSession sparkSession) {
        StructType structType = new StructType().add("Name", new StructType(
                new StructField[]{
                        new StructField("FirstName", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("MiddleName", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("LastName", DataTypes.StringType, false, Metadata.empty())
                }
        )).add(
                new StructField("ID", DataTypes.IntegerType, false, Metadata.empty())
        ).add(
                new StructField("Gender", DataTypes.StringType, false, Metadata.empty())
        ).add(
                new StructField("Salary", DataTypes.IntegerType, false, Metadata.empty())
        );
        List<Row> rows = List.of(
                RowFactory.create(RowFactory.create("James", "", "Smith"), "36636", "M", 3000),
                RowFactory.create(RowFactory.create("Michael", "Rose", ""), "40288", "M", 4000),
                RowFactory.create(RowFactory.create("Robert", "", "Williams"), "42114", "M", 4500),
                RowFactory.create(RowFactory.create("Maria", "Anne", "James"), "39192", "F", 3000),
                RowFactory.create(RowFactory.create("Jen", "Mary", "Brown"), "65654", "M", 4000)
        );

        final Dataset<Row> dataFrame = sparkSession.createDataFrame(rows, structType);
        dataFrame.printSchema();
    }

    public static Dataset<Row> usingArrayAndMapSchema(SparkSession sparkSession) {
        StructField[] nameStructField = new StructField[] {
                new StructField("FirstName", DataTypes.StringType, false, Metadata.empty()),
                new StructField("MiddleName", DataTypes.StringType, false, Metadata.empty()),
                new StructField("LastName", DataTypes.StringType, false, Metadata.empty())
        };
        StructType structType = new StructType()
                .add("Name", new StructType(nameStructField)).add("Hobbies", new ArrayType(DataTypes.StringType, false))
                .add("Properties", new MapType(DataTypes.StringType, DataTypes.StringType, false));

        Map<String, String> propOne = Maps.newHashMap();
        Map<String, String> propTwo = Maps.newHashMap();
        Map<String, String> propThree = Maps.newHashMap();
        Map<String, String> propFour = Maps.newHashMap();
        Map<String, String> propFive = Maps.newHashMap();

        propOne.put("Hair", "Black");
        propOne.put("eye", "Brown");

        propTwo.put("Skin", "Fair");
        propTwo.put("Nose", "Long");

        propThree.put("Hair", "Blonde");
        propThree.put("eye", "Green");

        propFour.put("Hair", "Black");
        propFour.put("eye", "Black");

        propFive.put("Hair", "red");
        propFive.put("eye", "gray");

        System.out.println(structType);

        List<Row> rows = List.of(
                RowFactory.create(RowFactory.create("James", "", "Smith"), List.of("Cricket", "Movies"), propOne),
                RowFactory.create(RowFactory.create("Michael", "Rose", ""), List.of("Tennis", "Study"), propTwo),
                RowFactory.create(RowFactory.create("Robert", "", "Williams"), List.of("Cooking", "Football"), propThree),
                RowFactory.create(RowFactory.create("Maria", "Anne", "James"), List.of("Java"), propFour),
                RowFactory.create(RowFactory.create("Jen", "Mary", "Brown"), List.of("Blogging"), propFive)
        );

        Dataset<Row> dataFrame = sparkSession.createDataFrame(rows, structType);
        dataFrame.printSchema();

        return dataFrame;
    }

    public static void flattenMap(Dataset<Row> dataFrame) {
        final String[] columns = dataFrame.columns();
        System.out.println("Columns are ::"+ Arrays.toString(columns));

        // get first row
        scala.collection.Map<String, String> properties = dataFrame.first().getAs("Properties");
        System.out.println(properties);


        Dataset<Row> dataset = dataFrame;
        final Map<String, String> stringStringMap = JavaConverters$.MODULE$.asJava(properties);
        for (String key :  stringStringMap.keySet()) {
            dataFrame = dataFrame.withColumn(key, dataFrame.col("Properties").getItem(key).as(key));
        }

        dataFrame.show();

        // Alternative way to flatten map - This will make union of all keys
        String[] keys =  {"Hair", "eye", "Skin", "Nose"};
        for (String key : keys) {
            dataset = dataset.withColumn(key, dataset.col("Properties").getItem(key));
        }
        dataset.show();
    }
}
