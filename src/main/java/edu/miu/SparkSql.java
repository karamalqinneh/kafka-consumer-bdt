package edu.miu;

import edu.miu.dto.Product;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkSql {
	private static final String TABLE_NAME = "retail_data";
	private static final String COLUMN_FAMILY_PRODUCT = "product_info";
	private static final String COLUMN_FAMILY_CUSTOMER = "customer_info";

	static Configuration config;
	static JavaSparkContext jsc;

	public static void main(String[] args) {
		SparkConf sconf = new SparkConf().setAppName("HBase").setMaster("local[*]");
		sconf.registerKryoClasses(new Class[]{org.apache.hadoop.hbase.io.ImmutableBytesWritable.class});

		config = HBaseConfiguration.create();
		config.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);

		jsc = new JavaSparkContext(sconf);
		SQLContext sqlContext = new SQLContext(jsc.sc());

		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = readTableByJavaPairRDD(jsc);
		System.out.println("Number of rows in hbase table: " + hBaseRDD.count());

		JavaRDD<Product> rows = hBaseRDD.map(row -> {
			Product product = new Product();
			byte[] rowKey = row._1.get();
			Result result = row._2;

			product.setInvoiceId(Bytes.toString(rowKey));
			product.setProductId(Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_PRODUCT), Bytes.toBytes("productId"))));
			product.setProductName(Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_PRODUCT), Bytes.toBytes("productName"))));
			product.setQuantity(Bytes.toInt(result.getValue(Bytes.toBytes(COLUMN_FAMILY_PRODUCT), Bytes.toBytes("quantity"))));
			product.setDate(Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_PRODUCT), Bytes.toBytes("date"))));
			product.setPrice(Bytes.toDouble(result.getValue(Bytes.toBytes(COLUMN_FAMILY_PRODUCT), Bytes.toBytes("price"))));

			// Extract customer info from 'customer_info' column family
			product.setCustomerId(Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_CUSTOMER), Bytes.toBytes("customerId"))));
			product.setCountry(Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY_CUSTOMER), Bytes.toBytes("country"))));

			return product;
		});

		DataFrame tableData = sqlContext.createDataFrame(rows, Product.class);
		tableData.registerTempTable(TABLE_NAME);
		tableData.printSchema();

		String sqlQuery1 = "SELECT * FROM retail_data WHERE quantity > 5";
		String sqlQuery2 = "SELECT * FROM retail_data WHERE quantity > 5 AND country = 'United Kingdom'";
		String sqlQuery3 = "SELECT country, SUM(quantity) AS TotalQuantity, SUM(quantity * price) AS TotalRevenue "
				+ "FROM retail_data GROUP BY country";

		DataFrame query2 = sqlContext.sql(sqlQuery1);
		query2.show();

		DataFrame query3 = sqlContext.sql(sqlQuery2);
		query3.show();

		DataFrame query4 = sqlContext.sql(sqlQuery3);
		query4.show();

		jsc.stop();
	}

	public static JavaPairRDD<ImmutableBytesWritable, Result> readTableByJavaPairRDD(JavaSparkContext jsc) {
		config = HBaseConfiguration.create();
		config.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);
		return jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
	}
}
