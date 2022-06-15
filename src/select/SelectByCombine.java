package select;

import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import utils.Utils;

public class SelectByCombine {
	
	static String[] situations = {};
	static String[] s_split = {};
	public static void SelectCombine(String situation) {
		situations = situation.split("\\+");
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		for(int i=0; i<situations.length; i++) {
			if(StringUtils.isEmpty(situations[i])) continue;
			s_split  = situations[i].split("\\|");
			for(String s: s_split) {
				System.out.println(s+"-"+i);
			}
			//设置过滤器
			switch(i) {
			case 0:
				FilterList filterTime = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				SingleColumnValueFilter filterStart = new SingleColumnValueFilter(Bytes.toBytes("time"), 
						Bytes.toBytes("time"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(s_split[0]));
				filterStart.setFilterIfMissing(true);
				SingleColumnValueFilter filterEnd = new SingleColumnValueFilter(Bytes.toBytes("time"), 
						Bytes.toBytes("time"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes(s_split[1]));
				filterEnd.setFilterIfMissing(true);
				filterTime.addFilter(filterStart);
				filterTime.addFilter(filterEnd);
				filterList.addFilter(filterTime);
				break;
			case 1:
				FilterList filterUids = new FilterList(FilterList.Operator.MUST_PASS_ONE);
				for(int j=0; j<s_split.length; j++ ) {
					SingleColumnValueFilter filterUid = 
							new SingleColumnValueFilter(Bytes.toBytes("uid"), Bytes.toBytes("uid"), CompareOp.EQUAL, Bytes.toBytes(s_split[j]));
					filterUid.setFilterIfMissing(true);
					filterUids.addFilter(filterUid);
				}
				filterList.addFilter(filterUids);
				break;
			case 2:
				FilterList filterKeys = new FilterList(FilterList.Operator.MUST_PASS_ONE);
				for(int k=0; k<s_split.length; k++ ) {
					SingleColumnValueFilter filterKey = 
							new SingleColumnValueFilter(Bytes.toBytes("key"), Bytes.toBytes("key"), CompareOp.EQUAL, new SubstringComparator(s_split[k]));
					filterKey.setFilterIfMissing(true);
					filterKeys.addFilter(filterKey);
				}
				filterList.addFilter(filterKeys);
				break;
			case 3:
				FilterList filterURLs = new FilterList(FilterList.Operator.MUST_PASS_ONE);
				for(int m=0; m<s_split.length; m++ ) {
					SingleColumnValueFilter filterUrl = 
							new SingleColumnValueFilter(Bytes.toBytes("url"), Bytes.toBytes("url"), CompareOp.EQUAL, new SubstringComparator(s_split[m]));
					filterUrl.setFilterIfMissing(true);
					filterURLs.addFilter(filterUrl);
				}
				filterList.addFilter(filterURLs);
				break;
			}
		}
		Utils.showFilter(filterList);
		Utils.close();
	}
}
