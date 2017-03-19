package cn.uway.ubp.monitor.data;

import java.util.List;
import java.util.Map;

import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.framework.data.model.GroupingArrayDataDescriptor;

/**
 * 一个网元对应的一组数据
 * 
 * @author liuwx
 */
public class GroupBlockData {

	/**
	 * 
	 * Map<网元主键,属于此网元的一组数据>
	 */
	private Map<String, List<GroupingArrayData>> groupingArrayDatas;

	/**
	 * 字段描述信息
	 */
	private GroupingArrayDataDescriptor metaInfo;

	/**
	 * 监控数据源的主键字段 用字符串表示 如果多个字段才能确定唯一主键 则使用"-"拼接在一起
	 */
	protected String indexKey;

	/**
	 * 要导出的字段key列表,使用"-"拼接在一起
	 */
	protected String exportFieldsKey;

	public Map<String, List<GroupingArrayData>> getGroupingArrayDatas() {
		return groupingArrayDatas;
	}

	public void setGroupingArrayDatas(Map<String, List<GroupingArrayData>> groupingArrayDatas) {
		this.groupingArrayDatas = groupingArrayDatas;
	}

	public GroupingArrayDataDescriptor getMetaInfo() {
		return metaInfo;
	}

	public void setMetaInfo(GroupingArrayDataDescriptor metaInfo) {
		this.metaInfo = metaInfo;
	}

	public String getIndexKey() {
		return indexKey;
	}

	public void setIndexKey(String indexKey) {
		this.indexKey = indexKey;
	}

	public String getExportFieldsKey() {
		return exportFieldsKey;
	}

	public void setExportFieldsKey(String exportFieldsKey) {
		this.exportFieldsKey = exportFieldsKey;
	}

}
