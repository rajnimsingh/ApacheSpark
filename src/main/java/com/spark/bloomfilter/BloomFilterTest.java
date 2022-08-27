package com.spark.bloomfilter;

import com.spark.tutorials.sql.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.util.sketch.BloomFilter;

public class BloomFilterTest {
    public static void main(String[] args) {
        SparkSession sparkSession = Main.buildSparkSession();

        Dataset<Row> dataset = sparkSession.read()
                .option("header", "true")
                .option("delimiter", "|")
                .csv("src/main/resources/data.csv");

        Dataset<Row> dataset2 = sparkSession.read()
                .option("header", "true")
                .option("delimiter", "|")
                .csv("src/main/resources/data.csv");

        dataset2.createOrReplaceTempView("DATA_TABLE");
        sparkSession.sql("select position from DATA_TABLE").show();

        BloomFilter bloomFilter = dataset.stat().bloomFilter(functions.col("position"), 100, 0.03);
        UDFCall udfCall = new UDFCall();

        sparkSession.udf().register("exists_udf", udfCall.mightContain(bloomFilter), DataTypes.BooleanType);

        sparkSession.sql("select exists_udf(position) from DATA_TABLE").show();

        dataset.show();
        sparkSession.close();
    }

}
