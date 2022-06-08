package main;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import jline.internal.InputStreamReader;
import utils.Utils;


/*
 * 载入文本中的日志数据，每行作为一条记录，
 * 分为访问时间，用户ID，查询词，返回结果排名，顺序号，URL这六个字段（列），
 * 存入HBASE。
 * */

public class DataClean {

	static String tableName = "SogouQ";
	static String[] ColFamily = {"time","uid","key","rank","no","url"};
	static ArrayList<String> rows = new ArrayList<>();
	static ArrayList<String[]> values = new ArrayList<>();
	
	public static void clean(String filePath) throws IOException {
		File file  = new File(filePath);
		DataInputStream in = new DataInputStream(new FileInputStream(file));
		BufferedReader buf = new BufferedReader(new InputStreamReader(in));
		
		String line;
		int count = 0;
		while((line = buf.readLine())!=null) {
			String[] v = line.split("\t| ");
			String row = "SQ"+String.valueOf(count);
			rows.add(row);
			values.add(v);
			count++;
		}
		buf.close();
		in.close();
		add();
	}
	
	public static void add() throws IOException {
		for (int i=0; i<rows.size(); i++) {
			addRecord(tableName, rows.get(i), ColFamily, values.get(i));
		}
	}
	
	public static void addRecord(String tableName,String row,String[] fields,String[] values) throws IOException {
		Utils.init();
		Table table = Utils.connection.getTable(TableName.valueOf(tableName));
		for(int i = 0;i < fields.length;i++){
			Put put = new Put(row.getBytes());
			String[] cols = fields[i].split(":");
//			System.out.println(fields[i]);
			put.addColumn(Bytes.toBytes(cols[0]), Bytes.toBytes(cols[0]), Bytes.toBytes(values[i]));
			table.put(put);
		}
		table.close();
		Utils.close();
	}
	
}
