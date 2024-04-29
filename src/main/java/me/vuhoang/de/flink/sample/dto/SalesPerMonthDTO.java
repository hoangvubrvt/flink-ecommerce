package me.vuhoang.de.flink.sample.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Date;

@Data
@AllArgsConstructor
public class SalesPerMonthDTO {

    private int year;
    private int month;
    private double totalSales;
}
