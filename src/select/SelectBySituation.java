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
		Scan scan = new Scan();
		scan.setFilter(filterList);
		Utils.close();
	}
	
	//根据关键字查询
	public static void SelectByKey(String situation) {
		
	}
	
	//根据关键字查询访问URL列表信息
	public static void SelectByKeyForURL(String situation) {
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
	}
}
