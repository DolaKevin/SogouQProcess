package select;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import utils.Utils;


public class SelectBySituation {

	static String[] situations = {};
	//根据开始和结束时间查询
	public static void SelectByTime(String situation) {
		situations = situation.split("\\|");
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		SingleColumnValueFilter filterStart = 
				new SingleColumnValueFilter(Bytes.toBytes("time"), Bytes.toBytes("time"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(situations[0]));
		filterStart.setFilterIfMissing(true);
		SingleColumnValueFilter filterEnd = 
				new SingleColumnValueFilter(Bytes.toBytes("time"), Bytes.toBytes("time"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes(situations[1]));
		filterEnd.setFilterIfMissing(true);
		filterList.addFilter(filterStart);
		filterList.addFilter(filterEnd);
		Utils.showFilter(filterList);
		Utils.close();
		
	}
	
	//根据用户id查询
	public static void SelectByUid(String situation) {
		situations = situation.split("\\|");
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		for(int i=0; i<situations.length; i++ ) {
			SingleColumnValueFilter filter = 
					new SingleColumnValueFilter(Bytes.toBytes("uid"), Bytes.toBytes("uid"), CompareOp.EQUAL, Bytes.toBytes(situations[i]));
			filter.setFilterIfMissing(true);
			filterList.addFilter(filter);
		}
		Utils.showFilter(filterList);
		Utils.close();
	}
	
	//根据关键字查询
	public static void SelectByKey(String situation) {
		situations = situation.split("\\|");
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		for(int i=0; i<situations.length; i++ ) {
			SingleColumnValueFilter filter = 
					new SingleColumnValueFilter(Bytes.toBytes("key"), Bytes.toBytes("key"), CompareOp.EQUAL, new SubstringComparator(situations[i]));
			filter.setFilterIfMissing(true);
			filterList.addFilter(filter);
		}
		Utils.showFilter(filterList);
		Utils.close();
	}
	
	//根据关键字查询访问URL列表信息
	public static void SelectByKeyForURL(String situation) {
		situations = situation.split("\\|");
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		for(int i=0; i<situations.length; i++ ) {
			SingleColumnValueFilter filter = 
					new SingleColumnValueFilter(Bytes.toBytes("url"), Bytes.toBytes("url"), CompareOp.EQUAL, new SubstringComparator(situations[i]));
			filter.setFilterIfMissing(true);
			filterList.addFilter(filter);
		}
		Utils.showFilter(filterList);
		Utils.close();
	}
}
