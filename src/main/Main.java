package main;

import java.io.IOException;
import java.util.Scanner;

import mapreduce.AccessCount;
import mapreduce.ConditionSelect;
import mapreduce.TimeCount;
import mapreduce.UserCount;
import select.SelectByCombine;
import select.SelectBySituation;

public class Main {
	
	public static void create() throws IOException {
		String TableName = "SogouQ";
		String[] ColFamily = {"time","uid","key","rank","no","url"};
		
		CreateTable.createTable(TableName, ColFamily);
	}
	
	
	public static void main(String[] args) throws Exception {
		int op;
		boolean flag = true;
		Scanner scan = new Scanner(System.in);
		String time = "";
		String uid = "";
		String key = "";
		String url = "";
		while (flag) {
			System.out.println("---------------------欢迎使用SogouQ数据服务-------------------");
			System.out.println("\t\t1.存储数据至Hbase");
			System.out.println("\t\t2.按照时间查询");
			System.out.println("\t\t3.按照uid查询");
			System.out.println("\t\t4.按照关键词查询");
			System.out.println("\t\t5.按照访问url查询");
			System.out.println("\t\t6.组合查询");
			System.out.println("\t\t7.条件查询");
			System.out.println("\t\t8.时段流量统计");
			System.out.println("\t\t9.用户使用频率统计");
			System.out.println("\t\t10.访问行为统计");
			System.out.println("\t\t0.退出系统");
			System.out.println("请输入操作码(0-10)：");
			op = scan.nextInt();
			switch(op){
			case 1:
				create();
				String filePath = "/home/hadoop/SogouQ1.reduced";
				DataClean.clean(filePath);
				break;
			case 2:
				//2.时间查询
				System.out.print("请输入时间范围(00:00:00-23:59:59)[起始时间与结束时间以\"|\"分隔]：");
				time = scan.next();
				SelectBySituation.SelectByTime(time);
				break;
			case 3:
				//1.uid查询
				System.out.print("请输入uid[多个uid以\"|\"分隔]：");
				uid = scan.next();
				SelectBySituation.SelectByUid(uid);
				break;
			case 4:
				//3.关键词查询
				System.out.print("请输入关键字[多个关键字以\"|\"分隔]：");
				key = scan.next();
				SelectBySituation.SelectByKey(key);
				break;
			case 5:
				//4.关键词查询URL
				System.out.print("请输入url(例：baidu)[多个url以\"|\"分隔]：");
				url = scan.next();
				SelectBySituation.SelectByKeyForURL(url);
				break;
			case 6:
				//5.组合查询
				System.out.print("请输入时间范围(00:00:00-23:59:59)[起始时间与结束时间以\"|\"分隔][不填写清填写\"|\"]：");
				time = scan.next();
				System.out.print("请输入uid[多个uid以\"|\"分隔][不填写清填写\"|\"]：");
				uid = scan.next();
				System.out.print("请输入关键字[多个关键字以\"|\"分隔][不填写清填写\"|\"]：");
				key = scan.next();
				System.out.print("请输入url(例：baidu)[多个url以\"|\"分隔][不填写清填写\"|\"]：");
				url = scan.next();
				String situation = time+"+"+uid+"+"+key+"+"+url;
				SelectByCombine.SelectCombine(situation);
				break;
			case 7:
				scan.nextLine();
				System.out.println("请输人时间段:");
				time = scan.nextLine();
				System.out.println("清输入用户:");
				uid=scan.nextLine();
				System.out.println("清输入关键词[]:");
				key=scan.nextLine();
				System.out.println("请输入网站");
				url = scan.nextLine();
				ConditionSelect.ConSelect(time, uid, key, url);
			case 8:
				scan.nextLine();
				System.out.println("请输人时间段:");
				time = scan.nextLine();
				TimeCount.TCount(time);
			case 9:
				UserCount.UCount();
			case 10:
				AccessCount.ACount();
			case 0:
				flag = false;
				break;
			default:
				System.out.println("输入操作指令有误，清重新输入!");
				break;
			}
		}
		scan.close();
	}
}
