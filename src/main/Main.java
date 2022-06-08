package main;

import java.io.IOException;

public class Main {
	
	public static void create() throws IOException {
		String TableName = "SogouQ";
		String[] ColFamily = {"time","uid","key","rank","no","url"};
		
		CreateTable.createTable(TableName, ColFamily);
	}
	
	public static void main(String[] args) throws IOException {
//		create();
		
		String filePath = "/home/hadoop/SogouQ.reduced";
		DataClean.clean(filePath);
	}
}
