package edu.miu.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDate;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Product {

    private String invoiceId;
    private String productId;
    private String productName;
    private int quantity;
    private String date;
    private double price;
    private String customerId;
    private String country;

}
