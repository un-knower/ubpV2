package cn.uway.ubp.monitor.indicator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cn.uway.framework.data.model.FieldIndexInfo;
import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.framework.el.extend.AbsDataProvider;
import cn.uway.framework.el.extend.ELRuntimeException;
import cn.uway.ubp.monitor.data.GroupBlockData;

public class MonitorDataProvider extends AbsDataProvider<GroupingArrayData> {

	protected GroupBlockData blockData;

	protected List<GroupingArrayData> lstMonitorDatas;

	private Map<String, FieldIndexInfo> fieldMapping;

	public MonitorDataProvider(String providerName, GroupBlockData blockData, List<GroupingArrayData> lstMonitorDatas) {
		super(providerName);

		this.blockData = blockData;
		this.fieldMapping = blockData.getMetaInfo().getFileIndexInfos();
		this.lstMonitorDatas = lstMonitorDatas;
	}

	public MonitorDataProvider(String providerName, Map<String, FieldIndexInfo> fieldMapping, List<GroupingArrayData> lstMonitorDatas) {
		super(providerName);

		this.blockData = null;
		this.fieldMapping = fieldMapping;
		this.lstMonitorDatas = lstMonitorDatas;
	}

	@Override
	public Object getValue(GroupingArrayData entry, String fieldName) throws ELRuntimeException {
		FieldIndexInfo fieldInfo = null;
		if (entry != null && (fieldInfo = fieldMapping.get(fieldName)) != null) {
			return entry.getPropertyValue(fieldInfo);
		} else if (entry == null)
			throw (new ELRuntimeException("MonitorDataProvider.getValue()，GroupingArrayData entry == null."));
		else
			throw (new ELRuntimeException("在MonitorDataProvider中，找不到名称为:\"" + fieldName + "\"的字段."));
	}

	@Override
	public Iterator<GroupingArrayData> createIterator() {
		if (lstMonitorDatas != null)
			return lstMonitorDatas.iterator();

		return null;
	}

	void setMonitorList(List<GroupingArrayData> lstMonitorDatas) {
		this.lstMonitorDatas = lstMonitorDatas;
	}

	public Object getOnceRecordFieldValue(String fieldName) {
		if (fieldName != null && lstMonitorDatas != null && lstMonitorDatas.size() > 0) {
			GroupingArrayData entry = lstMonitorDatas.iterator().next();
			if (entry != null) {
				try {
					return getValue(entry, fieldName.toLowerCase());
				} catch (ELRuntimeException e) {
					e.printStackTrace();
					return null;
				}
			}
		}

		return null;
	}

}
