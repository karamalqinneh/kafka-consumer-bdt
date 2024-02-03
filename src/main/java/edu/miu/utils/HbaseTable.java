package edu.miu.utils;

import edu.miu.dto.Product;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.format.DateTimeFormatter;

public class HbaseTable {

    public static Configuration config = HBaseConfiguration.create();
    public static Connection connection = null;


    private static final String TABLE_NAME = "retail_data";

    private static final byte[] CF_PRODUCT_INFO = "product_info".getBytes();
    private static final byte[] CF_CUSTOMER_INFO = "customer_info".getBytes();

    private static Table retailData;

    public static void init() {
        try {
            connection = ConnectionFactory.createConnection(config);
            retailData = connection.getTable(TableName.valueOf(TABLE_NAME));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static byte[] getBytes(String str) {
        if(str == null)
            return null;

        return Bytes.toBytes(str);
    }

    public static void populateData(Product product) throws IOException {
        Put row = new Put(product.getInvoiceId().getBytes());

        // Product Information
        row.addColumn(CF_PRODUCT_INFO, "productName".getBytes(), getBytes(product.getProductName()));
        row.addColumn(CF_PRODUCT_INFO, "quantity".getBytes(), getBytes(String.valueOf(product.getQuantity())));
        row.addColumn(CF_PRODUCT_INFO, "price".getBytes(), getBytes(String.valueOf(product.getPrice())));
        row.addColumn(CF_PRODUCT_INFO, "date".getBytes(), getBytes(product.getDate()));

        // Customer Information
        row.addColumn(CF_CUSTOMER_INFO, "customerId".getBytes(), getBytes(product.getCustomerId()));
        row.addColumn(CF_CUSTOMER_INFO, "country".getBytes(), getBytes(product.getCountry()));

        retailData.put(row);
    }
}
