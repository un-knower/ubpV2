package cn.uway.ubp.monitor.indicator.filter;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.data.model.FieldIndexInfo;
import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.framework.data.model.GroupingArrayDataDescriptor;
import cn.uway.ubp.monitor.data.BlockData;
import cn.uway.util.entity.ReplyTask;

public class RejectFilter extends AbstractFilter {

	private List<ReplyTask> replyTaskList;

	private Map<String, List<Date>> dateMap;

	private static final Logger logger = LoggerFactory
			.getLogger(RejectFilter.class);

	public RejectFilter(Timestamp currentMonitorTime, String fieldName,
			List<ReplyTask> replyTaskList, Map<String, List<Date>> dateMap) {
		super("驳回告警清除过滤器" + replyTaskList.toString());
		this.fieldName = fieldName;
		this.replyTaskList = replyTaskList;
		this.dateMap = dateMap;
	}

	/**
	 * 1 需要根据视图中的replytime 2 需要将BlockData中小于replytime的数据踢掉
	 */
	public BlockData doFilter(BlockData blockData) {
		// 剔除小于replytime 的数据

		BlockData block = initBlockData(blockData);
		Map<String, GroupingArrayData> filtedData = new HashMap<String, GroupingArrayData>();
		block.setGroupingArrayDatas(filtedData);

		Map<String, GroupingArrayData> rawData = blockData
				.getGroupingArrayDatas();
		Iterator<ReplyTask> iterator = replyTaskList.iterator();

		GroupingArrayDataDescriptor descriptor = blockData.getMetaInfo();
		logger.debug("==fieldName==is :{}", fieldName);
		FieldIndexInfo fieldInfo = null;
		if (StringUtils.isNotBlank(fieldName))
			fieldInfo = descriptor.getFileIndexInfo(fieldName.toLowerCase());

		while (iterator.hasNext()) {
			ReplyTask replyTask = (ReplyTask) iterator.next();
			GroupingArrayData data = rawData.get(replyTask.getNeSysID());

			if (data != null) {
				if (fieldInfo != null) {
					Object obj = data.getPropertyValue(fieldInfo);
					if (obj != null && obj instanceof java.util.Date) {
						if (((java.util.Date) obj).getTime() >= replyTask
								.getReplyTime().getTime())
							filtedData.put(replyTask.getNeSysID(), data);
					}
				} else {
					// 拿出dateMap 中的时间区间来和 replyTime 比较
					Set<String> alias = dateMap.keySet();
					Iterator<String> aliasIterator = alias.iterator();
					while (aliasIterator.hasNext()) {
						String aliasName = aliasIterator.next();
						List<Date> dateList = dateMap.get(aliasName);
						for (Date date : dateList) {
							if (date.getTime() >= replyTask.getReplyTime()
									.getTime()) {
								filtedData.put(replyTask.getNeSysID(), data);
								logger.debug("==data==is :{}and neSysID is:{}",
										data, replyTask.getNeSysID());
							}
						}
					}
				}
			}
		}
		block.setGroupingArrayDatas(filtedData);
		return block;

	}
}
