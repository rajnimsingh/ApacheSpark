package com.spark.dataimport;

import java.io.*;

public class DataImport {
    public static void main(String[] args) throws IOException {
        try (FileInputStream fis = new FileInputStream(new File("src/main/resources/orders/newOrdersData.numbers"))) {
            try(FileOutputStream fout = new FileOutputStream(new File("src/main/resources/orders/orders.csv"))) {
                System.out.println("Start data writing...");
                int data;
                while ((data = fis.read()) != -1) {
                    fout.write(data);
                    fout.write(",".getBytes());
                }
                System.out.println("Data has been written to the file.");
            }
        }
    }
}
