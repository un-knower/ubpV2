package cn.uway.ubp.monitor.indicator.filter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.data.model.FieldIndexInfo;
import cn.uway.framework.data.model.FieldType;
import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.framework.data.model.GroupingArrayDataDescriptor;
import cn.uway.ubp.monitor.data.BlockData;

/**
 * 数据过滤，剔除给出的数据
 * 
 * @author zhouq Date 2013-6-18
 */
public class ExcludFilter extends AbstractFilter {

	private static final Logger logger = LoggerFactory.getLogger(ExcludFilter.class);

	public ExcludFilter(String fieldName, String fieldValueStr, String prototype) {
		super(prototype);
		this.fieldName = fieldName;
		setFieldValues(fieldValueStr);
	}

	@Override
	public BlockData doFilter(BlockData blockData) {
		if (fieldValues == null || fieldValues.size() == 0)
			return blockData;
		String indexKeyNames = blockData.getIndexKey();
		Map<String, GroupingArrayData> rawData = blockData.getGroupingArrayDatas();
		Map<String, GroupingArrayData> filtedData = new HashMap<String, GroupingArrayData>();
		BlockData block = initBlockData(blockData);
		// 如果过滤字段即BlockData的索引字段 可以快速检索
		if (indexKeyNames != null && indexKeyNames.equals(fieldName)) {
			Iterator<String> iterator = fieldValues.iterator();
			while (iterator.hasNext()) {
				String index = iterator.next();
				rawData.remove(index);

			}
			return blockData;
		}
		GroupingArrayDataDescriptor descriptor = blockData.getMetaInfo();
		FieldIndexInfo indexInfo = descriptor.getFileIndexInfo(fieldName.toLowerCase());
		if (indexInfo == null) {
			logger.error("在数据源中找不到字段名：{}", fieldName);
			return blockData;
		}
		Set<Map.Entry<String, GroupingArrayData>> entryset = rawData.entrySet();
		Iterator<Entry<String, GroupingArrayData>> iterator = entryset.iterator();
		while (iterator.hasNext()) {
			Entry<String, GroupingArrayData> next = iterator.next();
			GroupingArrayData data = next.getValue();
			String index = next.getKey();
			Object obj = data.getPropertyValue(indexInfo);
			if (obj == null)
				continue;
			// 数据类型转换
			if (indexInfo.getFieldType().equals(FieldType.STRING)) {
				if (!fieldValues.contains(obj))
					filtedData.put(index, data);
				continue;
			}

			if (obj != null && obj instanceof Number) {
				Long num = ((Number) obj).longValue();
				String val = num.toString();

				if (!fieldValues.contains(val))
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
			// if (!bMatched)
			// filtedData.put(index, data);
		}

		block.setGroupingArrayDatas(filtedData);
		return block;
	}
}
