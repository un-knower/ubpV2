package cn.uway.ubp.monitor.indicator.filter;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import cn.uway.ubp.monitor.data.BlockData;

/**
 * 包含关系的过滤器<br>
 * 
 * @author chenrongqiang 2013-6-14
 */
public class AbstractFilter implements IFilter {

	/**
	 * 需要过滤的字段名称
	 */
	protected String fieldName;

	protected Set<String> fieldValues;

	protected String filterText;

	protected String prototype;

	public AbstractFilter() {
	}

	public AbstractFilter(String prototype) {
		this.prototype = prototype;
	}

	public Set<String> getFieldValues() {
		return fieldValues;
	}

	public void setFieldValues(String string) {
		if (StringUtils.isBlank(string)) {
			return;
		}
		String[] values = string.split(",");
		if (values.length == 0)
			return;
		fieldValues = new HashSet<String>();
		for (String value : values) {
			value = value.trim();
			if (value.length() < 1)
				continue;
			fieldValues.add(value);
		}
	}

	/**
	 * 初始化fieldValues set
	 */
	public void init(Connection taskConn) throws Exception {

	}

	public BlockData doFilter(BlockData blockData) {
		return null;
	}

	BlockData initBlockData(BlockData blockData) {
		BlockData block = new BlockData();
		block.setMetaInfo(blockData.getMetaInfo());
		block.setIndexKey(blockData.getIndexKey());
		block.setExportFieldsKey(blockData.getExportFieldsKey());
		return block;
	}

	@Override
	public void destroy() {

	}

	@Override
	public String toString() {
		return prototype;
	}

}
