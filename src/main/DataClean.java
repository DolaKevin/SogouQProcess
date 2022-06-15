package main;

import java.io.BufferedReader;
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
		Utils.init();
		Table table = Utils.connection.getTable(TableName.valueOf(tableName));
		File file  = new File(filePath);
		FileInputStream in = new FileInputStream(file);
		BufferedReader buf = new BufferedReader(new InputStreamReader(in,"utf-8"));
		
		String line;
		int count = 1;
		String[] v = null;
		String row = null;
		while((line = buf.readLine())!=null) {
			if (count<1724266) {
				v = line.split("\t| ");
				row = "SQ"+String.valueOf(count);
				Put put = new Put(row.getBytes());
				for(int i = 0;i < ColFamily.length;i++){
					String[] cols = ColFamily[i].split(":");
					put.addColumn(Bytes.toBytes(cols[0]), Bytes.toBytes(cols[0]), Bytes.toBytes(v[i]));
				}
				table.put(put);
			}
			count++;
			
		}
		buf.close();
		in.close();
		table.close();
		Utils.close();
	}
}
