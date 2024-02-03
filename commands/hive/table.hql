CREATE EXTERNAL TABLE retail_data (
                                      invoiceId String,
                                      productName String,
                                      quantity Int,
                                      price Double,
                                      date String,
                                      customerId String,
                                      country String
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES (
        'hbase.columns.mapping'=':key,product_info:productName,product_info:quantity,product_info:price,product_info:date,customer_info:customerId,customer_info:country'
    )
    TBLPROPERTIES(
        'hbase.table.name'='retail_data',
        'hbase.mapred.output.outputtable'='retail_data'
    );