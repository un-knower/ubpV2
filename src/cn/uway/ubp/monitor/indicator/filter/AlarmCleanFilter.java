package cn.uway.ubp.monitor.indicator.filter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.data.model.FieldIndexInfo;
import cn.uway.framework.data.model.FieldType;
import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.framework.data.model.GroupingArrayDataDescriptor;
import cn.uway.ubp.monitor.data.BlockData;

/**
 * 闭环任务网元过滤器 功能类似于IncludeFitler,其实将功能硬性的合并到其中,也是完全可以的
 * 
 * @author Chris @ 2013-8-11
 */
public class AlarmCleanFilter extends AbstractFilter {

	private static final Logger logger = LoggerFactory
			.getLogger(AlarmCleanFilter.class);

	public AlarmCleanFilter(String fieldName, List<String> alarmClearIdList) {
		super("告警清除过滤器" + alarmClearIdList.toString());
		this.fieldName = fieldName;

		fieldValues = new HashSet<String>();

		if (alarmClearIdList != null && !alarmClearIdList.isEmpty()) {

			for (String alarmClearId : alarmClearIdList) {
				fieldValues.add(alarmClearId);
			}
		}
	}

	@Override
	public BlockData doFilter(BlockData blockData) {
		BlockData block = initBlockData(blockData);
		Map<String, GroupingArrayData> filtedData = new HashMap<String, GroupingArrayData>();
		block.setGroupingArrayDatas(filtedData);

		// 如果没有需要过滤网元
		if (fieldValues == null || fieldValues.isEmpty()) {
			return block;
		}

		String indexKeyNames = blockData.getIndexKey();
		// if (indexKeyNames == null ||
		// !indexKeyNames.equalsIgnoreCase(fieldName)) {
		// return block;
		// }

		Map<String, GroupingArrayData> rawData = blockData
				.getGroupingArrayDatas();

		// 如果监控主键和数据源主键相同，则直接匹配
		if (indexKeyNames != null && indexKeyNames.equals(fieldName)) {
			Iterator<String> iterator = fieldValues.iterator();
			while (iterator.hasNext()) {
				String index = iterator.next();
				GroupingArrayData data = rawData.get(index);
				if (data != null)
					filtedData.put(index, data);
			}
			block.setGroupingArrayDatas(filtedData);
			return block;
		}

		GroupingArrayDataDescriptor descriptor = blockData.getMetaInfo();
		FieldIndexInfo indexInfo = descriptor.getFileIndexInfo(fieldName
				.toLowerCase());
		if (indexInfo == null) {
			logger.error("在数据源中找不到字段名：{}", fieldName);
			return block;
		}
		Set<String> entryset = rawData.keySet();
		Iterator<String> iterator = entryset.iterator();
		while (iterator.hasNext()) {
			String index = iterator.next();
			GroupingArrayData data = rawData.get(index);
			Object obj = data.getPropertyValue(indexInfo);
			if (obj == null)
				continue;

			// 数据类型转换
			if (indexInfo.getFieldType().equals(FieldType.STRING)) {
				if (fieldValues.contains(obj))
					filtedData.put(index, data);
				continue;
			}

			if (obj != null && obj instanceof Number) {
				Long num = ((Number) obj).longValue();
				String val = num.toString();

				if (fieldValues.contains(val))
					filtedData.put(index, data);
				continue;
			} else {
				logger.warn("非String、整型、长整形不能作为key");
			}
		}
		// 将指定的网元数据留下,其他丢掉
		// Map<String, GroupingArrayData> rawData =
		// blockData.getGroupingArrayDatas();
		// Iterator<String> iterator = fieldValues.iterator();
		// while (iterator.hasNext()) {
		// String index = iterator.next();
		// GroupingArrayData data = rawData.get(index);
		// if (data != null)
		// filtedData.put(index, data);

		return block;
	}

}
