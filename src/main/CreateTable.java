package main;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import utils.Utils;

/*
 * 根据传入的表名tableName，列族colFamily
 * 创建表
 * */

public class CreateTable {
	
	public static void createTable(String tableName, String[] colFamily) throws IOException {
		Utils.init();
		TableName name = TableName.valueOf(tableName);
		if(Utils.admin.tableExists(name)) {
			System.out.println("Table "+tableName+" exists!");
		} else {
			TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(name);
			for(String str : colFamily){
				ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(str)).build();
				builder.setColumnFamily(family);
			}
			Utils.admin.createTable(builder.build());
		}
		Utils.close();
	}
}
