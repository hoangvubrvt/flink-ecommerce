package me.vuhoang.de.flink.sample.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Date;

@Data
@AllArgsConstructor
public class SalesPerDayDTO {
    private Date transactionDate;
    private double totalSales;
}
