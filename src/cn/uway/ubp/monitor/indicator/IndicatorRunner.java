package cn.uway.ubp.monitor.indicator;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.framework.data.model.GroupingArrayDataDescriptor;
import cn.uway.framework.el.extend.AbsDataProvider;
import cn.uway.framework.el.extend.AbsExcuteAccepter;
import cn.uway.framework.el.extend.FormulaExpression;
import cn.uway.ubp.monitor.MonitorConstant;
import cn.uway.ubp.monitor.data.GroupBlockData;
import cn.uway.ubp.monitor.event.Event;
import cn.uway.ubp.monitor.event.EventBlockData;
import cn.uway.util.entity.Indicator;

public class IndicatorRunner extends AbsExcuteAccepter {

	private Map<Date, Map<String, Object>> subIndicatorValues = null;

	private ExpressionParam exprParam;

	private static final Logger logger = LoggerFactory.getLogger(IndicatorRunner.class);

	// 指标运算接口
	public IndicatorResult execute(ExpressionParam exprParam, boolean bCreateAlarmOnlyFormularInTrueCase, Map<String, Date> nesysReplayTimeMap) {
		this.exprParam = exprParam;

		// 指标公式执行返回信息
		IndicatorResult result = new IndicatorResult();
		// 事件信息块
		EventBlockData eventBlockData = new EventBlockData();
		// 数据时间
		eventBlockData.setDataTime(exprParam.getDataTime());
		// 设置event Blcok
		result.setEventBlockData(eventBlockData);
		// 设置monitorTask任务信息
		// result.setMonitorTaskStatus(monitorTaskStatus);
		// 开始进行指标表达式计算时间
		// monitorTaskStatus.setStartExpressionDate(new Date());

		// 如果没有指定数据源，则直接返回，并告知失败原因是
		if (exprParam.rawData.entrySet().size() < 1) {
			// 表达式运算状态(0表示失败, 1表示成功)
			result.setStatus(0);
			// 失败原因
			result.setCause("输入的参数不正确，数据源未指定");
			// // 事件个数
			// monitorTaskStatus.setEventNum(0);
			// // 完成指标表达式计算时间
			// monitorTaskStatus.setEndExpressionDate(new Date());
			return result;
		}

		try {
			// 主分组Block
			GroupBlockData primaryGroupBlockData = null;
			// 主数据源provider提供者
			MonitorDataProvider primaryMonitorDataProvider = null;
			// 创建数据Provider;
			List<MonitorDataProvider> lstProviders = new ArrayList<MonitorDataProvider>();
			Iterator<Entry<String, GroupBlockData>> iterBlockData = exprParam.rawData.entrySet().iterator();
			// 将每一个BlockData根据key名称，都生成一个provider
			while (iterBlockData.hasNext()) {
				Entry<String, GroupBlockData> entry = iterBlockData.next();
				String providerKey = entry.getKey();
				GroupBlockData groupBlockData = entry.getValue();
				/*
				 * 取数据源中的第一个分组作为主分组； 因为各数据源之间是inner join关系，
				 * 所以取任何一个分组作为主分组来检索和遍历数据都可以。
				 */
				if (primaryGroupBlockData == null)
					primaryGroupBlockData = groupBlockData;

				MonitorDataProvider provider = new MonitorDataProvider(providerKey, groupBlockData, null);
				lstProviders.add(provider);

				if (primaryMonitorDataProvider == null)
					primaryMonitorDataProvider = provider;
			}

			// 根据公式，创建IndicatorExpression对象
			List<FormulaExpression> lstFormulaExprs = new ArrayList<FormulaExpression>();
			List<Indicator> indicatorList = exprParam.getIndicatorList();
			for (Indicator indicator : indicatorList) {
				// 表达式为空，则跳过
				if (StringUtils.isBlank(indicator.getContent()))
					continue;

				FormulaExpression formularExpr = new FormulaExpression(indicator.getContent(), this);
				// 将数据源全部注册到FormulaExpression中
				for (int j = 0; j < lstProviders.size(); ++j)
					formularExpr.registerDataProvider(lstProviders.get(j));

				lstFormulaExprs.add(formularExpr);
			}

			// 遍历主数据块，取得每一个数据分组，进行运算
			Integer nIgnoreItemCount = 0;
			Iterator<Entry<String, List<GroupingArrayData>>> iterPrimaryKey = primaryGroupBlockData.getGroupingArrayDatas().entrySet().iterator();
			while (iterPrimaryKey.hasNext()) {
				Entry<String, List<GroupingArrayData>> primaryEntry = iterPrimaryKey.next();
				// 数据组的key
				String primaryKey = primaryEntry.getKey();

				// 设置主数据分组的monitorData list到provider;
				primaryMonitorDataProvider.setMonitorList(primaryEntry.getValue());

				// 对所有的表达式进行计算
				for (int i = 0; i < lstFormulaExprs.size(); ++i) {
					FormulaExpression formularExpr = lstFormulaExprs.get(i);

					// 将其它数据分组按primaryKey进行inner关联
					iterBlockData = exprParam.rawData.entrySet().iterator();
					// 跳过第一个数据集合，第一个是primaryBlockData，已经取出来了;
					if (iterBlockData.hasNext())
						iterBlockData.next();

					// 找出其数据分组里面index key相同的monitorData list到对应的provider
					boolean bInnerJoin = true;
					int blockDataIndex = 0;
					while (iterBlockData.hasNext()) {
						Entry<String, GroupBlockData> entry = iterBlockData.next();
						MonitorDataProvider currProvider = lstProviders.get(++blockDataIndex);

						// 如果不是公式不需要用到的数据源，则不参与join运算
						if (!formularExpr.isValidProvider(currProvider.getProviderName()))
							break;

						// 设置每一个Provider的数据源
						GroupBlockData groupBlockData = entry.getValue();
						List<GroupingArrayData> lstCurrBlockMonitorDatas = groupBlockData.getGroupingArrayDatas().get(primaryKey);
						if (lstCurrBlockMonitorDatas != null) {
							currProvider.setMonitorList(lstCurrBlockMonitorDatas);
						} else {
							// 如果找不到对应的分组，代表不符号inner join条件.
							bInnerJoin = false;
							break;
						}
					}
					// inner join的数据源缺少一或多个，这样的不参与计算
					if (!bInnerJoin) {
						++nIgnoreItemCount;
						continue;
					}

					// 清除上一个公式，已计算好的子指标值
					// this.subIndicatorValues.clear();
					subIndicatorValues = new HashMap<Date, Map<String, Object>>();
					// 运行表达式，评估计算结果
					Object ret = formularExpr.evaluate();
					// 在同一个指标中，只要第一个 表达式满足了，就不需要继续计算了
					boolean bRet = (ret instanceof Boolean && (Boolean) ret) ? true : false;
					if (bRet || !bCreateAlarmOnlyFormularInTrueCase) {
						// 告警极别
						short nAlarmLevel = 0;
						if (!bRet && !bCreateAlarmOnlyFormularInTrueCase) {
							// 驳回告警，如果数据时间在回单时间之前， 则不产生驳回事件
							Date replayDate = nesysReplayTimeMap.get(primaryKey);
							if (replayDate == null || exprParam.getDataTime().getTime() <= replayDate.getTime()) {
								continue;
							}
							nAlarmLevel = Event.INVALID_LEVEL;
						}else if (bRet && !bCreateAlarmOnlyFormularInTrueCase) {
							// 告警清除，如果数据时间在回单时间之前， 则不产生驳回事件
							Date replayDate = nesysReplayTimeMap.get(primaryKey);
							if (replayDate == null || exprParam.getDataTime().getTime() <= replayDate.getTime()) {
								continue;
							}
							if (exprParam.getIndicatorList().get(i) != null){
								nAlarmLevel = exprParam.getIndicatorList().get(i).getAlarmLevel();
							}
						}else if (exprParam.getIndicatorList().get(i) != null) {
							nAlarmLevel = exprParam.getIndicatorList().get(i).getAlarmLevel();
						}
						
						// 创建event事件
						Event ev = new Event();
						ev.setLevel(nAlarmLevel);
						// 事件数据的group-key、value
						ev.setIndexKey(primaryGroupBlockData.getIndexKey());
						ev.setIndexValues(primaryKey);
						// 子指标值
						ev.setIndicatorValues(subIndicatorValues);
						ev.setDataTime(exprParam.getDataTime());

						String exportFieldKey = primaryGroupBlockData.getExportFieldsKey();
						String[] exportFields = null;
						// 分解要导出的字段名
						if (exportFieldKey != null)
							exportFields = exportFieldKey.split(MonitorConstant.KEY_SPLIT);
						// 导出所有的附加信息
						if (exportFields != null && exportFields.length > 0) {
							Map<String, Object> mapExportFieldsValue = new HashMap<String, Object>();
							for (int j = 0; j < exportFields.length; ++j) {
								String fieldName = exportFields[j];
								if (fieldName == null)
									continue;

								fieldName = fieldName.trim();
								if (fieldName.length() < 1)
									continue;

								mapExportFieldsValue.put(fieldName.toLowerCase(), primaryMonitorDataProvider.getOnceRecordFieldValue(fieldName));
							}

							ev.setExportFieldsValue(mapExportFieldsValue);
						}

						// 将新生成的event加入到event Blcok中
						eventBlockData.setEvent(ev);

						break;
					}
				}
			} // end of while

			// Log.debug("任务{}，参与计算的对象个数{}，因为数据源关联不到而忽略的对象个数{}，产生的告警数{}",
			// new Object[]{monitorTaskStatus.getMonitorTask().getTaskId(),
			// nCalcItemCount,
			// nIgnoreItemCount,
			// eventBlockData.size()});

			// 表达式运算状态(0表示失败, 1表示成功)
			result.setStatus(1);
			// 事件个数
			// monitorTaskStatus.setEventNum(eventBlockData.size());
			// 完成指标表达式计算时间s
			// monitorTaskStatus.setEndExpressionDate(new Date());
		} catch (Exception e) {
			// 表达式运算状态(0表示失败, 1表示成功)
			result.setStatus(0);
			// 失败原因
			result.setCause(e.getMessage());
		}

		// 更新任务状态表状态
		// MonitorTaskStatusDAO.instance().updateMonitorTaskStatus(monitorTaskStatus,
		// 0);

		return result;
	}

	@Override
	public void onSubExpressionCalculated(String keyName, Object value) {
		/**
		 * <pre>
		 * 本方法回as函数回调使用
		 * 通常当as函数中表达式值为null的时候，引发该异常
		 * </pre>
		 */
		if (subIndicatorValues == null || value == null || !(value instanceof Number || value instanceof String)) {
			logger.debug("未处理的指标值：name={} value={}", new Object[]{keyName, value});
			return;
		}

		String[] keyRageNames = keyName.split("[$]");
		Date indicatorDate = null;
		String indicatorName = null;
		if (keyRageNames.length == 2) {
			indicatorDate = this.exprParam.getDateByRangeName(keyRageNames[0]);
			indicatorName = keyRageNames[1];
		} else {
			indicatorDate = this.exprParam.getDateByRangeName(null);
			indicatorName = keyName;
		}

		Map<String, Object> mapIndicatorValues = subIndicatorValues.get(indicatorDate);
		if (mapIndicatorValues == null) {
			mapIndicatorValues = new HashMap<String, Object>();
			subIndicatorValues.put(indicatorDate, mapIndicatorValues);
		}
		mapIndicatorValues.put(indicatorName, value);

//		if (value instanceof String)
//			mapIndicatorValues.put(indicatorName, value);
//		else
//			mapIndicatorValues.put(indicatorName, ((Number) value).doubleValue());
	}

	public static Object execute(String expression, GroupingArrayData groupingArrayData, GroupingArrayDataDescriptor groupingArrayDataDesc)
			throws Exception {
		List<GroupingArrayData> lst = new ArrayList<GroupingArrayData>(1);
		lst.add(groupingArrayData);

		return execute(expression, lst, groupingArrayDataDesc);
	}

	public static Object execute(String expression, List<GroupingArrayData> lstGroupingArrayDatas, GroupingArrayDataDescriptor groupingArrayDataDesc)
			throws Exception {
		MonitorDataProvider provider = new MonitorDataProvider("default", groupingArrayDataDesc.getFileIndexInfos(), lstGroupingArrayDatas);

		return execute(expression, provider);
	}

	public static Object execute(String expression, AbsDataProvider<?> provider) throws Exception {
		FormulaExpression formularExpr = new FormulaExpression(expression, null);
		formularExpr.registerDataProvider(provider);
		Object ret = formularExpr.evaluate();

		return ret;
	}

}
