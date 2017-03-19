package cn.uway.ubp.monitor.indicator.filter;

import java.util.HashMap;
import java.util.Iterator;
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
 * 数据过滤，保留给出的数据
 * 
 * @author zhouq Date 2013-6-18
 */
public class IncludFilter extends AbstractFilter {

	private static final Logger logger = LoggerFactory.getLogger(IncludFilter.class);

	public IncludFilter(String fieldName, String fieldValueStr, String prototype) {
		super(prototype);
		this.fieldName = fieldName;
		setFieldValues(fieldValueStr);
	}

	@Override
	public BlockData doFilter(BlockData blockData) {
		Map<String, GroupingArrayData> rawData = blockData.getGroupingArrayDatas();
		Map<String, GroupingArrayData> filtedData = new HashMap<String, GroupingArrayData>();
		BlockData block = initBlockData(blockData);

		// 包含过滤器，如果被包含的东西的对象不存在，则返回空
		if (fieldValues == null || fieldValues.size() == 0) {
			logger.debug("被过滤的数据为空！");
			block.setGroupingArrayDatas(filtedData);
			return block;
		}

		String indexKeyNames = blockData.getIndexKey();
		logger.debug("过滤器关键字段名{}，数据源字段名{}，指定的字段个数{}", new Object[]{fieldName, indexKeyNames, fieldValues.size()});
		// 如果过滤字段即BlockData的索引字段 可以快速检索
		if (indexKeyNames != null && indexKeyNames.equals(fieldName)) {
			Iterator<String> iterator = fieldValues.iterator();
			while (iterator.hasNext()) {
				String index = iterator.next();
				GroupingArrayData data = rawData.get(index);
				if (data != null)
					filtedData.put(index, data);
				// else
				// LOGGER.debug("找不到主键=={}的数据记录.", new Object[]{index});
			}
			block.setGroupingArrayDatas(filtedData);
			return block;
		}

		logger.debug("过滤器关键字段名{}，数据源字段名{}, 字段名不一样，将采用遍历查找方法.", fieldName, indexKeyNames);
		GroupingArrayDataDescriptor descriptor = blockData.getMetaInfo();
		FieldIndexInfo indexInfo = descriptor.getFileIndexInfo(fieldName.toLowerCase());
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

			// boolean bMatched = false;
			// for(String value : fieldValues){
			// switch (indexInfo.getFieldType()) {
			// case FieldType.DOUBLE :
			// if (Double.valueOf(value).equals(obj)) {
			// bMatched = true;
			// }
			//
			// break;
			// case FieldType.FLOAT :
			// if (Float.valueOf(value).equals(obj)) {
			// bMatched = true;
			// }
			//
			// break;
			// case FieldType.LONG :
			// if (Long.valueOf(value).equals(obj)) {
			// bMatched = true;
			// }
			//
			// break;
			// case FieldType.INT :
			// if (Integer.valueOf(value).equals(obj)) {
			// bMatched = true;
			// }
			//
			// break;
			// case FieldType.SHORT :
			// if (Short.valueOf(value).equals(obj)) {
			// bMatched = true;
			// }
			//
			// break;
			// case FieldType.BYTE :
			// if (Byte.valueOf(value).equals(obj)) {
			// bMatched = true;
			// }
			//
			// break;
			// }
			//
			// if (bMatched)
			// break;
			// }
			//
			// if (bMatched) {
			// filtedData.put(index, data);
			// }
		}

		block.setGroupingArrayDatas(filtedData);
		return block;
	}

}
