package cn.uway.ubp.monitor.data;

import java.util.Date;
import java.util.List;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.framework.data.model.GroupingArrayDataDescriptor;
import cn.uway.framework.util.StringUtil;
import cn.uway.ubp.monitor.context.BusyHourManager;
import cn.uway.ubp.monitor.context.Configuration;

/**
 * 忙时抽象处理
 * 
 * @author zqing @ 2013-9-18
 */
public abstract class AbstractBusy implements Busy {

	enum BusyType {
		// G网忙时
		Netype_G,
		// C网忙时
		NetType_C,
		// 非忙时
		NetType_N;
	}

	// 默认为非忙时
	BusyType busyType = BusyType.NetType_N;

	protected static final Logger logger = LoggerFactory.getLogger(AbstractBusy.class);

	// 包含忙时的节点
	String filter;

	// G/W忙时字段名
	String fieldG;

	// G/W过滤级别
	int valueG;

	// C网忙时过滤级别表
	int levelC;

	// C网忙时过滤忙时时段类型
	int typeC;

	// 主键值
	String index;

	// 过滤时间
	Date date;

	// 数据对象
	GroupingArrayData groupingArrayData;

	// 数据描述信息
	GroupingArrayDataDescriptor groupingArrayDataDescripto;

	/**
	 * C网忙时 缓存配置启用
	 */
	private boolean ENABLE;

	public AbstractBusy(String filter) throws DocumentException {
		ENABLE = Configuration.getBoolean(Configuration.BUSY_HOUR_ENABLE);
		this.filter = filter;
		parseFilter();
	}

	/**
	 * 忙时处理 时间过滤掉 返回true 保留数据 返回false 忙时的数据保留下来是参与规则运算，满足规则出告警数据 包含数据返回false;
	 */
	public boolean onBusy(GroupingArrayData groupingArrayData, GroupingArrayDataDescriptor groupingArrayDataDescripto, String index)
			throws DocumentException {
		if (!ENABLE)
			return false;
		this.groupingArrayData = groupingArrayData;
		this.groupingArrayDataDescripto = groupingArrayDataDescripto;
		this.index = index;
		switch (busyType) {
			case Netype_G :
				return onBusyByG();
			case NetType_C :
				return onBusyByC();
			default :
				return false;
		}
	}

	/**
	 * C网忙时省市特殊处理
	 */
	public boolean onBusy(int cityId) {
		if (!ENABLE)
			return false;
		if (BusyHourManager.BUSY_MAP == null)
			return false;
		if (BusyHourManager.BUSY_MAP.get(levelC) == null)
			return false;
		if (BusyHourManager.BUSY_MAP.get(levelC).get(typeC) == null)
			return false;
		List<String> cityIdList = BusyHourManager.BUSY_MAP.get(levelC).get(typeC).get(date);
		if (cityIdList == null)
			return false;
		if (cityIdList.contains(String.valueOf(cityId)))
			return false;
		return true;
	}

	/**
	 * G网忙时处理
	 * 
	 * @return
	 */
	protected abstract boolean onBusyByG();

	/**
	 * C网忙时处理
	 * 
	 * @return
	 */
	protected abstract boolean onBusyByC();

	/**
	 * 解析忙时信息
	 * 
	 * @throws DocumentException
	 */
	private void parseFilter() throws DocumentException {
		// 如果任务信息xml为空，直接抛出空指针异常
		if (StringUtil.isEmpty(filter))
			return;
		Document doc = DocumentHelper.parseText(filter);
		Element root = doc.getRootElement();
		Element element = root.element("busy-hour");
		if (element == null)
			return;
		Attribute fieldAttribute = element.attribute("field");
		if (fieldAttribute != null) {
			busyType = BusyType.Netype_G;
			fieldG = fieldAttribute.getValue();
			valueG = Integer.valueOf(element.attribute("value").getValue());
			return;
		}
		Attribute levelAttribute = element.attribute("level");
		if (levelAttribute != null) {
			busyType = BusyType.NetType_C;
			levelC = Integer.valueOf(levelAttribute.getValue());
			typeC = Integer.valueOf(element.attribute("type").getValue());
			return;
		}
	}

	/**
	 * 数据处理时间
	 */
	public void setDate(Date date) {
		this.date = date;
	}

}
