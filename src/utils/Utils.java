package utils;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;

public class Utils {
	public static Configuration configuration;
	public static Connection connection;
	public static Admin admin;
	
	//建立连接
	public static void init() {
		configuration = new Configuration();
		HBaseConfiguration.addHbaseResources(configuration);
		configuration.set("hbase.rootdir", "hdfs://Master:9000/hbase");
		configuration.set("hbase.zookeeper.quorum", "Slave1");
	    configuration.set("hbase.zookeeper.property.clientPort", "2181");
		try {
			connection = ConnectionFactory.createConnection(configuration);
			admin = connection.getAdmin();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
		
	//结束连接
	public static void close() {
		try {
			if(admin != null) {
				admin.close();
			}
			if(null != connection) {
				connection.close();
			}
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Table getTable() {
		return getTable("SogouQ");
	}
	
	public static Table getTable(String name) {
		
		Table table = null;
		try {
			if(StringUtils.isEmpty(name)) return null;
			table = connection.getTable(TableName.valueOf(name));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return table;
	}
	
	public static void showResult(Result result) {
		//表格扫描器
		CellScanner cellScanner = result.cellScanner();
		try {
			while(cellScanner.advance()) {
				Cell cell = cellScanner.current();
				
				//列族
				System.out.println(new String(CellUtil.cloneFamily(cell),"utf-8"));
				//列名
				System.out.println(new String(CellUtil.cloneQualifier(cell),"utf-8"));
				//列值
				System.out.println(new String(CellUtil.cloneValue(cell),"utf-8"));
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void showScan(Table table, Scan scan) {
		try {
			//结果扫描器
			ResultScanner resultScanner = table.getScanner(scan);
			//进行迭代
			Iterator<Result> iterator = resultScanner.iterator();
			while(iterator.hasNext()) {
				Result result = iterator.next();
				showResult(result);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void showFilter(Filter filter) {
		Scan scan = new Scan();
		scan.setFilter(filter);
		Table table = getTable();
		showScan(table,scan);
	}
}
