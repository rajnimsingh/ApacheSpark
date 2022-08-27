package com.spark.bloomfilter;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.util.sketch.BloomFilter;

import java.io.Serializable;

public class UDFCall implements Serializable {

    public UDF1<String, Boolean> udfExists() {
        return this::exists;
    }

    public boolean exists(String value) {
        return StringUtils.isNotBlank(value) && value.equalsIgnoreCase("Developer");
    }
    public UDF1<String, Boolean> mightContain(BloomFilter bloomFilter) {
        return new UDF1<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return bloomFilter.mightContain(s);
            }
        };
    }

}
