package cn.uway.ubp.monitor.data;

import java.io.Serializable;
import java.util.Map;

import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.framework.data.model.GroupingArrayDataDescriptor;

/**
 * 
 * 监控数据缓存对象
 * 
 * @author zhouq Date :2013-5-21
 */
public class BlockData implements Serializable {

	/**
	 * UUID
	 */
	private static final long serialVersionUID = 9115826378534508444L;

	/**
	 * 监控数据源的主键字段 对应的值,如果多个字段才能确定唯一主键 则将值使用"-"拼接在一起 GroupingArrayData一条记录
	 */
	private Map<String, GroupingArrayData> groupingArrayDatas;

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

	public Map<String, GroupingArrayData> getGroupingArrayDatas() {
		return groupingArrayDatas;
	}

	public void setGroupingArrayDatas(Map<String, GroupingArrayData> groupingArrayDatas) {
		this.groupingArrayDatas = groupingArrayDatas;
	}

	public String getIndexKey() {
		return indexKey;
	}

	public void setIndexKey(String indexKey) {
		this.indexKey = indexKey;
	}

	public GroupingArrayDataDescriptor getMetaInfo() {
		return metaInfo;
	}

	public void setMetaInfo(GroupingArrayDataDescriptor metaInfo) {
		this.metaInfo = metaInfo;
	}

	public String getExportFieldsKey() {
		return exportFieldsKey;
	}

	public void setExportFieldsKey(String exportFieldsKey) {
		this.exportFieldsKey = exportFieldsKey;
	}

}