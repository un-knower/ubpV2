package cn.uway.ubp.monitor.event;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 监控时间对象定义
 * 
 * @author chenrongqiang 2013-5-28
 */
public class Event implements Serializable {

	/**
	 * UUID
	 */
	private static final long serialVersionUID = 6108610628038636172L;
	
	public static final short INVALID_LEVEL = 9999;

	/**
	 * 监控数据源的主键字段 用字符串表示 如果多个字段才能确定唯一主键 则使用"-"拼接在一起
	 */
	protected String indexKey;

	/**
	 * 监控数据源的主键字段 对应的值,如果多个字段才能确定唯一主键 则将值使用"-"拼接在一起
	 */
	protected String indexValues;

	/**
	 * 指标级别 与告警级别定义相同 在产生告警时将会根据默认的告警级别传入
	 */
	protected short level;

	/**
	 * 事件对应的数据时间
	 */
	protected Date dataTime;

	// /**
	// * 在指标表达式运算过程中产生的指标计算结果 key为指标的名称 value位表达式运算过程中产生的表达式计算结果
	// */
	// protected Map<String, Double> indicatorValues = new HashMap<String,
	// Double>();

	/**
	 * 要导出的字段信息列表 key:字段名称 value:值 sample: ne_cell_name 某某某小区名称 city_name 城市名称
	 */
	protected Map<String, Object> exportFieldsValue = new HashMap<String, Object>();

	/**
	 * 指标值 key: 数据日期 value: {[指标名][值],[指标名][值], [指标名][值]}
	 */
	protected Map<Date, Map<String, Object>> indicatorValues = new HashMap<Date, Map<String, Object>>();

	public short getLevel() {
		return level;
	}

	public void setLevel(short level) {
		this.level = level;
	}

	public String getIndexKey() {
		return indexKey;
	}

	public void setIndexKey(String indexKey) {
		this.indexKey = indexKey;
	}

	public String getIndexValues() {
		return indexValues;
	}

	public void setIndexValues(String indexValues) {
		this.indexValues = indexValues;
	}

	public Map<Date, Map<String, Object>> getIndicatorValues() {
		return indicatorValues;
	}

	public void setIndicatorValues(Map<Date, Map<String, Object>> indicatorValues) {
		this.indicatorValues = indicatorValues;
	}

	public Date getDataTime() {
		return dataTime;
	}

	public void setDataTime(Date dataTime) {
		this.dataTime = dataTime;
	}

	public Map<String, Object> getExportFieldsValue() {
		return exportFieldsValue;
	}

	public void setExportFieldsValue(Map<String, Object> exportFieldsValue) {
		this.exportFieldsValue = exportFieldsValue;
	}

}
