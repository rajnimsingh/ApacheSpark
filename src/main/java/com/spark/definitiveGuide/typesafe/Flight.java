package com.spark.definitiveGuide.typesafe;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class Flight {
    private String destinationCountryName;
    private String originCountryName;
    private int count;
}
