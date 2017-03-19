package cn.uway.ubp.monitor.indicator.filter;

import java.sql.Connection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.ubp.monitor.data.BlockData;

/**
 * 过滤器处理器<br>
 * 持有本次监控任务的Filter列表。并且对每条记录都进行filter处理<br>
 * 
 * @author chenrongqiang 2013-6-14
 */
public class FilterHandler {

	/**
	 * 过滤器集合
	 */
	protected List<IFilter> filterList;

	/**
	 * 需要参与过滤器的数据
	 */
	protected BlockData blockData;

	/**
	 * 表达式运算过程信息，此处用于数据记录加载过滤器信息
	 */
	protected StringBuilder experience;

	public FilterHandler(List<IFilter> filterList, BlockData blockData,
			StringBuilder experience) {
		this.filterList = filterList;
		this.blockData = blockData;
		this.experience = experience;
	}

	/**
	 * 过滤监控数据<br>
	 * 
	 * @return BlockData 过滤后的数据
	 * @throws Exception
	 */
	public BlockData handle(Connection taskConn) throws Exception {
		if (filterList == null || filterList.isEmpty())
			return blockData;

		Iterator<IFilter> iterator = this.filterList.iterator();
		experience.append("<filter-list>");
		int flag = 1;
		while (iterator.hasNext()) {
			IFilter filter = iterator.next();
			filter.init(taskConn);
			int count1 = blockData.getGroupingArrayDatas().size();

			blockData = filter.doFilter(blockData);

			int count2 = blockData.getGroupingArrayDatas().size();
			filter.destroy();

			experience.append("<filter in=\"" + count1 + "\" out=\"" + count2
					+ "\">");
			String filterStr = filter.toString();
			if (StringUtils.isNotBlank(filterStr)) {
				filterStr = filterStr.replace("]]>", "]]]]><![CDATA[");
			}
			experience.append("<name><![CDATA[" + filterStr + "]]></name>");

			if (count2 <= 0)
				break;

			// 列出所有主键，通常是网元
			experience.append("<index-list>");
			if (count2 > 0) {
				experience.append("<![CDATA[");
				Map<String, GroupingArrayData> dataMap = blockData
						.getGroupingArrayDatas();

				for (String key : dataMap.keySet()) {
					// 数据量超长的时候要加换行
					if (flag % 100 == 0)
						experience.append("\n");
					experience.append(key).append(",");
					// 在粒度为小时的时候，为了不影响性能每个小时的输出记录最多记录80000个
					// 超过8万个记录数据库CLOB字段会记录为 <Value Error>
					if (flag++ > 80000)
						break;
				}
				experience.append("]]>");
			}
			experience.append("</index-list>");

			experience.append("</filter>");

		}

		experience.append("</filter-list>");

		return blockData;
	}
}
