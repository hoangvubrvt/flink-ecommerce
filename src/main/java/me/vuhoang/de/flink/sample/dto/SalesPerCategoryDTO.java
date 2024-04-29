package me.vuhoang.de.flink.sample.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Date;

@Data
@AllArgsConstructor
public class SalesPerCategoryDTO {
    private Date transactionDate;
    private String category;
    private double totalSales;
}
