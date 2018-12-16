//import java.util.Arrays;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase
//import org.apache.spark.sql.
import scala.Tuple2;

public class SparkOnHbase {
	//private static Object String;

	public static void main(String[] args) { 
		  
	  SparkConf conf =new SparkConf().setMaster("local").setAppName("Json_Reader"); 
		  conf.set("spark.sql.crossJoin.enabled", "true");
		  JavaSparkContext javaSparkContext = new JavaSparkContext(conf); 
		
		  
		  SparkSession sparkSession = SparkSession
			      .builder()
			      .master("local")
				  .appName("SparkonHbase")
				  .getOrCreate();
		  Configuration hconf = HBaseConfiguration.create();
		  //hconf.set("hbase.master", "localhost:59139");
		  hconf.set(TableInputFormat.INPUT_TABLE, "RiskSense1");
		  //hconf.set("hbase-site.xml\"");
		  hconf.addResource("hbase-site.xml");
	     // String tableName = "HBase.CounData1_Raw_Min1";
		  TableName tName= TableName.valueOf("RiskSense1");
		  JavaPairRDD<ImmutableBytesWritable, Result> source;
		  /*= javaSparkContext.newAPIHadoopRDD(hconf, TableInputFormat.class,
                  ImmutableBytesWritable.class, Result.class);*/
		  HTableDescriptor htable = new HTableDescriptor(tName);
		  Table table;
		  //Put put;
	      try
	      {
	    	  //	HBaseAdmin hadmin = new HBaseAdmin(hconf);
	    	  	Dataset<Row> df = sparkSession.read().parquet("/Users/senthilkumar/Documents/Senthil/RiskSense/asset/RiskSense_data.parquet").toDF();
			df.printSchema();
			//JavaPairRDD<String,String> assetsContentsRDD = df.javaRDD();
			
			/*df.map(new Function<Tuple2<String, String>, String>() {
				@Override
				public String call(Tuple2<String, String> assetContent) throws Exception {
					return assetContent._2();
       
				}
			});*/
			//df.write().options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
	    	  	
	    	  	Connection conn = ConnectionFactory.createConnection(hconf);
	    	  	Admin hadmin = conn.getAdmin();
	    	  	
	    	  	if(!hadmin.isTableAvailable(tName))
	    	  	{	
	    	  		System.out.println("Table "+tName+" doesn't exists...");
	    	  		System.out.println("Creating Table "+tName+"...");
	    	  		 
	      	  	htable.addFamily( new HColumnDescriptor("Network"));
	      	  	//htable.addFamily( new HColumnDescriptor("fqdn"));
	    	  		hadmin.createTable(htable);
	    	  		source = javaSparkContext.newAPIHadoopRDD(hconf, TableInputFormat.class,
	    	                  ImmutableBytesWritable.class, Result.class);
	    	  		JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = df.javaRDD().mapToPair(
	  				      new PairFunction<Row, ImmutableBytesWritable, Put>() {
	  				    	  	//@Override
	  				    	  	public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
	  				         
	  				    	  		Put put = new Put(Bytes.toBytes(row.getString(0)));
	  				    	  		put.addColumn(Bytes.toBytes("Network"), Bytes.toBytes("dns"), Bytes.toBytes(row.getString(1)));
	  				    	  		put.addColumn(Bytes.toBytes("Network"), Bytes.toBytes("fqdn"), Bytes.toBytes(row.getString(2)));
	  				    	  		put.addColumn(Bytes.toBytes("Network"), Bytes.toBytes("ip"), Bytes.toBytes(row.getString(3)));
	  				    	  		put.addColumn(Bytes.toBytes("Network"), Bytes.toBytes("port"), Bytes.toBytes(row.getString(4)));
	  				    	  		put.addColumn(Bytes.toBytes("Network"), Bytes.toBytes("qid"), Bytes.toBytes(row.getString(5)));
	  				    	  		put.addColumn(Bytes.toBytes("Network"), Bytes.toBytes("severity_level"), Bytes.toBytes(row.getString(6)));
	  				    	  		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);     
	  				    }
	  			});
	    	  		
	    	  		if(source.count()>0)
	    	  		{
	    	  			Job newAPIJobConfiguration1 = Job.getInstance(hconf);	
		    	  		newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "RiskSense1");
		    	  		newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
	    	  			System.out.println("Inserting into HBase Table...");
		    	  		System.out.println("Data: "+hbasePuts.collect());
			  		hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
	    	  		}
	    	  		else
	    	  		{
	    	  			System.out.println("Parquet Data is empty ...");
	    	  		}
	    	  	   // table = conn.getTable(tName);
	    	  	    //table.put(put);
	    	  	}
	    	  	else
	    	  	{
	    	  		System.out.println("Table "+tName+" already exists..");
	    	  		Scan scan = new Scan();
	    	  		source = javaSparkContext.newAPIHadoopRDD(hconf, TableInputFormat.class,
	    	                  ImmutableBytesWritable.class, Result.class);
	    	  		
	    	  		JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = df.javaRDD().mapToPair(
		  				      new PairFunction<Row, ImmutableBytesWritable, Put>() {
		  				    	  	//@Override
		  				    	  	public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
		  				         
		  				    	  		Put put = new Put(Bytes.toBytes(row.getString(0)));
		  				    	  		put.addColumn(Bytes.toBytes("Network"), Bytes.toBytes("dns"), Bytes.toBytes(row.getString(1)));
		  				    	  		put.addColumn(Bytes.toBytes("Network"), Bytes.toBytes("fqdn"), Bytes.toBytes(row.getString(2)));
		  				    	  		put.addColumn(Bytes.toBytes("Network"), Bytes.toBytes("ip"), Bytes.toBytes(row.getString(3)));
		  				    	  		put.addColumn(Bytes.toBytes("Network"), Bytes.toBytes("port"), Bytes.toBytes(row.getString(4)));
		  				    	  		put.addColumn(Bytes.toBytes("Network"), Bytes.toBytes("qid"), Bytes.toBytes(row.getString(5)));
		  				    	  		put.addColumn(Bytes.toBytes("Network"), Bytes.toBytes("severity_level"), Bytes.toBytes(row.getString(6)));
		  				    	  		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);     
		  				    }
		  			});
	    	  		Job newAPIJobConfiguration1 = Job.getInstance(hconf);	
	    	  		newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "RiskSense1");
	    	  		newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
	    	  		if(source.count()>0)
	    	  		{
	    	  			System.out.println("Inserting into HBase Table...");
		    	  		System.out.println("Data: "+hbasePuts.collect());
			  		hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
	    	  		}
	    	  		else
	    	  		{
	    	  			System.out.println("Parquet Data is empty ...");
	    	  		}
	    	  		
		  		//hbasePuts.sav
		  			
	    	  		ResultScanner scanner = conn.getTable(tName).getScanner(scan);
	    	  		//System.out.println(hadmin.listTableNames());
	    	  		System.out.println("Checking record count of table "+tName+": "+source.count());
	    	  		//System.out.println(source.collect());;
	    	  		
	    	  		//Input Data is not matching properly so parquet table is created as empty always...
	    	  		//So not able to perform rest of the questions..
	    	  		Result result;
	    	  		for (result = scanner.next(); result != null; result = scanner.next())
	    	  		{
	    	  			System.out.println("Found row : " + (result.getValue(Bytes.toBytes("Network"), Bytes.toBytes("dns"))).toString());
	    	  	      //closing the scanner
	    	  			System.out.println("Found row : " + result.getRow().toString());
	    	  		
	    	  		}
	    	  		//result.getRow().toString().
	    	  	      scanner.close();
	    	  	
	    	  		
	    	  	}
	    	  		
	   }
	      catch(IOException e)
	      {
	    	  	System.out.println("Exception in Hbase Admin" + e);
	      }
	      
	     
	  }
	  
}