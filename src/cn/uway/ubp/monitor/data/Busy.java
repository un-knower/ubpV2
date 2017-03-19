package cn.uway.ubp.monitor.data;

import java.util.Date;

import org.dom4j.DocumentException;

import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.framework.data.model.GroupingArrayDataDescriptor;

/**
 * 忙时接口
 * 
 * @author zqing @ 2013-9-24
 */
public interface Busy {

	/**
	 * 忙时处理 时间过滤掉 返回true 保留数据 返回false
	 */
	boolean onBusy(GroupingArrayData groupingArrayData, GroupingArrayDataDescriptor groupingArrayDataDescripto, String index)
			throws DocumentException;

	/**
	 * C网忙时省市特殊处理
	 */
	boolean onBusy(int cityId);

	/**
	 * 数据处理时间
	 */
	public void setDate(Date date);
}
