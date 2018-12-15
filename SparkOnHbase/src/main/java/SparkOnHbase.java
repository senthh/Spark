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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase
//import org.apache.spark.sql.
import scala.Tuple2;

public class SparkOnHbase {
	private static Object String;

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
		  hconf.set(TableInputFormat.INPUT_TABLE, "test3");
		  //hconf.set("hbase-site.xml\"");
		  hconf.addResource("hbase-site.xml");
	     // String tableName = "HBase.CounData1_Raw_Min1";
		  TableName tName= TableName.valueOf("test3");
		  JavaPairRDD<ImmutableBytesWritable, Result> source;
		  /*= javaSparkContext.newAPIHadoopRDD(hconf, TableInputFormat.class,
                  ImmutableBytesWritable.class, Result.class);*/
		  HTableDescriptor htable = new HTableDescriptor(tName);
		  Table table;
	      try
	      {
	    	  //	HBaseAdmin hadmin = new HBaseAdmin(hconf);
	    	  	 
	    	  	Connection conn = ConnectionFactory.createConnection(hconf);
	    	  	Admin hadmin = conn.getAdmin();
	    	  	if(!hadmin.isTableAvailable(tName))
	    	  	{	
	    	  		System.out.println("Table "+tName+" doesn't exists...");
	    	  		System.out.println("Creating Table "+tName+"...");
	    	  		 
	      	  	htable.addFamily( new HColumnDescriptor("Id"));
	      	  	htable.addFamily( new HColumnDescriptor("Name"));
	    	  		hadmin.createTable(htable);
	    	  		Put p = new Put(Bytes.toBytes("row1"));
	    	  		//p.addColumn(family, qualifier, value)
	    	  		p.addColumn(Bytes.toBytes("Id"), Bytes.toBytes("col1"),Bytes.toBytes("1"));
	    	  	    p.addColumn(Bytes.toBytes("Name"),Bytes.toBytes("col2"),Bytes.toBytes("SenthilKumar"));
	    	  	    table = conn.getTable(tName);
	    	  	    table.put(p);
	    	  	}
	    	  	else
	    	  	{
	    	  		System.out.println("Table "+tName+" already exists..");
	    	  		Scan scan = new Scan();
	    	  		source = javaSparkContext.newAPIHadoopRDD(hconf, TableInputFormat.class,
	    	                  ImmutableBytesWritable.class, Result.class);
	    	  		scan.addColumn(Bytes.toBytes("Id"), Bytes.toBytes("col1"));
	    	  		scan.addColumn(Bytes.toBytes("Name"), Bytes.toBytes("col2"));
	    	  		ResultScanner scanner = conn.getTable(tName).getScanner(scan);
	    	  		//System.out.println(hadmin.listTableNames());
	    	  		System.out.println("Checking record count of table "+tName+": "+source.count());
	    	  		//System.out.println(source.collect());;
	    	  		Result result;
	    	  		for (result = scanner.next(); result != null; result = scanner.next())

	    	  	    System.out.println("Found row : " + (result.getValue(Bytes.toBytes("Name"), Bytes.toBytes("col2"))).toString());
	    	  	      //closing the scanner
	    	  		System.out.println("Found row : " + result);
	    	  	      scanner.close();
	    	  	
	    	  		
	    	  	}
	    	  		
	   }
	      catch(IOException e)
	      {
	    	  	System.out.println("Exception in Hbase Admin" + e);
	      }
	      
	     
	  }
	  
}