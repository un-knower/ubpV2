package cn.uway.ubp.monitor.data;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.DocumentException;

import cn.uway.ubp.monitor.MonitorConstant;
import cn.uway.ubp.monitor.context.BusyHourManager;

/**
 * C网及G网忙时处理 配置忙时过滤：忙时的数据保留下来是参与规则运算，满足规则出告警数据
 * 
 * @author zqing @ 2013-9-24
 */
public class BusyImpl extends AbstractBusy {

	public BusyImpl(String filter) throws DocumentException {
		super(filter);
	}

	@Override
	protected boolean onBusyByG() {
		Object obj = groupingArrayData
				.getPropertyValue(groupingArrayDataDescripto
						.getFileIndexInfo(fieldG));
		if (obj == null)
			throw new IllegalArgumentException("字段" + fieldG + "不正确");
		// 数据位与等于0为忙时数据 ，数据保留
		return (((Integer) obj) & valueG) != 0;
	}

	@Override
	protected boolean onBusyByC() {
		String[] indexArr = index.split(MonitorConstant.KEY_SPLIT);
		List<Object> indexList = new ArrayList<Object>();
		for (String idx : indexArr) {
			Object obj = groupingArrayData
					.getPropertyValue(groupingArrayDataDescripto
							.getFileIndexInfo(idx.toLowerCase()));
			if (obj instanceof String) {
				indexList.add(obj);
				continue;
			}

			String stringNumber = "0";
			if (obj != null)
				stringNumber = String.valueOf(obj);

			BigDecimal number = new BigDecimal(stringNumber);
			indexList.add(number.toString());
		}
		String indexValue = StringUtils.join(indexList,
				MonitorConstant.KEY_SPLIT);
		if (BusyHourManager.BUSY_MAP == null)
			return false;
		if (BusyHourManager.BUSY_MAP.get(levelC) == null)
			return false;
		if (BusyHourManager.BUSY_MAP.get(levelC).get(typeC) == null)
			return false;
		List<String> cityIdList = BusyHourManager.BUSY_MAP.get(levelC)
				.get(typeC).get(date);
		if (cityIdList == null)
			return false;
		if (cityIdList.contains(indexValue))
			return false;
		return true;
	}
}
