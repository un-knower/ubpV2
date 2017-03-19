package cn.uway.ubp.monitor.rule.job;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.util.DateTimeUtil;
import cn.uway.ubp.monitor.MonitorConstant;
import cn.uway.ubp.monitor.context.Configuration;
import cn.uway.ubp.monitor.dao.AlarmDAO;
import cn.uway.ubp.monitor.dao.ExportMapDAO;
import cn.uway.ubp.monitor.dao.MonitorTaskStatusDAO;
import cn.uway.ubp.monitor.dao.RejectDAO;
import cn.uway.ubp.monitor.event.Event;
import cn.uway.ubp.monitor.indicator.IndicatorResult;
import cn.uway.ubp.monitor.indicator.filter.FilterBuilder;
import cn.uway.ubp.monitor.rule.Alarm;
import cn.uway.ubp.monitor.rule.AlarmDetail;
import cn.uway.ubp.monitor.rule.handler.EventProvider;
import cn.uway.ubp.monitor.rule.handler.frequency.FrequencyRuleHandler;
import cn.uway.ubp.monitor.rule.handler.level.LevelRuleHandler;
import cn.uway.ubp.monitor.rule.handler.top.TopRuleHandler;
import cn.uway.util.DateGranularityUtil;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.ExportMap;
import cn.uway.util.entity.MonitorTask;
import cn.uway.util.entity.OccurTimes;
import cn.uway.util.entity.Rule;
import cn.uway.util.enums.TimeUnit;

/**
 * 规则运算工作线程
 * 
 * @author chenrongqiang 2013-5-28
 */
public class RuleWork {

	/**
	 * 指标运算结果
	 */
	private IndicatorResult indicatorResult;

	/**
	 * 规则对应的任务信息
	 */
	private MonitorTask monitorTask;

	/**
	 * 规则运算结果
	 */
	private RuleJobFuture ruleJobFuture = new RuleJobFuture();

	/**
	 * Memo分隔字符
	 */
	private static final char US = '\31';

	private static final Logger logger = LoggerFactory
			.getLogger(RuleWork.class);

	public RuleWork(MonitorTask monitorTask, IndicatorResult indicatorResult) {
		this.monitorTask = monitorTask;
		this.indicatorResult = indicatorResult;
	}

	public RuleJobFuture call(Connection alarmConn, Connection taskConn, List<Date> eventTimeList) {
		long taskId = monitorTask.getTaskId();
		logger.debug(
				"任务{},监控时间{},规则运算线程启动",
				new Object[]{monitorTask.getTaskId(),
						monitorTask.getCurrMonitorTime()});
		ruleJobFuture.setStartDate(new Date());
		ruleJobFuture.setTaskId(taskId);

		Rule rule = monitorTask.getRule();

		if (!checkHandleRule(alarmConn, taskConn, rule, eventTimeList)) {
			ruleJobFuture.setStatus(0);
			ruleJobFuture.setCause("告警存储数据异常");
			ruleJobFuture.setEndDate(new Date());
			return ruleJobFuture;
		}

		// 告警输出
		ruleJobFuture.setStatus(200);
		ruleJobFuture.setEndDate(new Date());

		return ruleJobFuture;
	}

	// 处理每个event的计算结果，筛选出要导出的指标
	private void processEventIndicators(
			final Map<Date, Map<String, Object>> indicatorValues,
			List<String> lstIndicatorNames, List<AlarmDetail> lstDetails)
			throws Exception {
		Iterator<Entry<Date, Map<String, Object>>> iterDate = indicatorValues
				.entrySet().iterator();
		while (iterDate.hasNext()) {
			Entry<Date, Map<String, Object>> entry = iterDate.next();
			Date datetime = entry.getKey();
			Map<String, Object> mapValues = entry.getValue();

			// 找出当前时间点的exportDataDetail对象，如没有，则创建一个
			AlarmDetail alarmDetail = null;
			Iterator<AlarmDetail> iterFind = lstDetails.iterator();
			while (iterFind.hasNext()) {
				AlarmDetail currDetailInfo = iterFind.next();
				if (currDetailInfo.getDatetime().compareTo(datetime) == 0) {
					alarmDetail = currDetailInfo;
					break;
				}
			}

			if (alarmDetail == null) {
				alarmDetail = new AlarmDetail();
				alarmDetail.setDatetime(datetime);
				lstDetails.add(alarmDetail);
			}

			if (mapValues.values().size() < 1) {
				String piName = monitorTask.getPiName();
				if (piName == null || piName.trim().length()<1) {
					piName = "主指标";
				}
				
				if (!lstIndicatorNames.contains(piName))
					lstIndicatorNames.add(piName);
				
				alarmDetail.getIndicatorValues().put(piName, "null");
			} else {
				// 将每个指标的值存放到exportDataDetail对象中
				Iterator<Entry<String, Object>> iter = mapValues.entrySet()
						.iterator();
				while (iter.hasNext()) {
					Entry<String, Object> indiEntry = iter.next();
					String name = indiEntry.getKey();
					Object value = indiEntry.getValue();

					// 提取指标名称到lstIndicatorNames中
					if (!lstIndicatorNames.contains(name))
						lstIndicatorNames.add(name);

					alarmDetail.getIndicatorValues().put(name, value);
				}
			}
		}
	}

	/**
	 * 创建告警<br>
	 * 
	 * @param alarmConn
	 * @param taskConn
	 * @param alarmEvents
	 * @return monitorTask.getTaskId()
	 * @throws Exception
	 */
	private List<Alarm> createAlarm(Connection alarmConn, Connection taskConn, Map<String, List<Event>> alarmEvents)
			throws Exception {
		DataSource dataSource = monitorTask.getDataSource();

		List<Alarm> alarms = new LinkedList<Alarm>();
		List<ExportMap> exportMapList = ExportMapDAO.getInstance()
				.getExportMaps(taskConn);
		Set<String> keySet = alarmEvents.keySet();
		for (String index : keySet) {
			List<Event> events = alarmEvents.get(index);
			Alarm alarm = new Alarm();
			Event lastEvent = null;

			List<String> lstIndicatorNames = new ArrayList<String>();
			List<AlarmDetail> alarmDetails = new ArrayList<AlarmDetail>();

			/*if (index.equalsIgnoreCase("1100030031272200")) {
				assert(false);
			}*/
			
			// 告警级别判断
			Date startTime = null;
			Date EndTime = null;
			// 住指标最后时间的主指标取值
			String monitorValue = "0";
			short level = MonitorConstant.SERIOUS;
			StringBuilder sb = new StringBuilder();
			for (Event event : events) {
				lastEvent = event;
				Map<Date, Map<String, Object>> mapIndicatorValues = event
						.getIndicatorValues();
				processEventIndicators(mapIndicatorValues, lstIndicatorNames,
						alarmDetails);
				
				// 取最高级别
				if (level < event.getLevel())
					level = event.getLevel();
			}
			
			// 按照日期，将子记录排序
			Collections.sort(alarmDetails,
					new AlarmDetail.AlarmDetailComparator());
			// 指标名称列表
			alarm.setIndicatorNames(lstIndicatorNames);
			// 每一条详细记录的数据值结果
			alarm.setDetailDatas(alarmDetails);
			
			// 对于没有告警情情的，设置一个默认值(驳回告警中，有些主指标值为null, 不满足闭环条件驳回的)
			if (alarmDetails.size() < 1) {
				startTime = monitorTask.getCurrMonitorTime();
				EndTime = monitorTask.getCurrMonitorTime();
				monitorValue = "0";
			}

			// 输出每一天的详细指标值
			for (AlarmDetail detail : alarmDetails) {
				Date currDataTime = detail.getDatetime();
				if (startTime == null)
					startTime = currDataTime;

				if (EndTime == null) {
					EndTime = currDataTime;

					if (StringUtils.isNotBlank(monitorTask.getPiName())) {
						monitorValue = String.valueOf(detail
								.getIndicatorValues().get(
										monitorTask.getPiName().trim()));
					}
				}

				if (currDataTime.before(startTime))
					startTime = currDataTime;

				if (currDataTime.after(EndTime)) {
					EndTime = currDataTime;
					if (StringUtils.isNotBlank(monitorTask.getPiName())) {
						monitorValue = String.valueOf(detail
								.getIndicatorValues().get(
										monitorTask.getPiName().trim()));
					}
				}
				// {数据时间} {指标值}[/{指标值}...],
				sb.append("\n")
						.append(DateTimeUtil.formatDateTime(currDataTime))
						.append(" "); // 数据时间
				for (String indicatorName : lstIndicatorNames) {
					if (detail.getIndicatorValues().containsKey(indicatorName)) {
						Object value = detail.getIndicatorValues().get(
								indicatorName);
						sb.append(value).append("/");
					} else {
						// 对于未计算的指标值，用"-"标识在告警正文中
						sb.append(" － ").append("/");
					}
				}

				if (lstIndicatorNames.size() > 0)
					sb.deleteCharAt(sb.length() - 1);
				sb.append(",");
			}

			if (sb.length() > 0)
				sb.deleteCharAt(sb.length() - 1);

			// 指标取值
			String eventStr = sb.toString();
			StringBuilder sb2 = new StringBuilder();
			Map<String, Object> exportValue = lastEvent.getExportFieldsValue();
			String neName = "";
			String cityId = "0";
			for (Entry<String, Object> entry : exportValue.entrySet()) {
				for (ExportMap export : exportMapList) {
					String exportField = export.getExportField().toLowerCase();
					String mapField = export.getMapField().toLowerCase();
					if (entry.getKey().equalsIgnoreCase(exportField)) {
						if (mapField.equalsIgnoreCase("CITY_ID")) {
							cityId = exportValue.get(exportField) == null
									? "0"
									: exportValue.get(exportField).toString();
						} else if (mapField.equalsIgnoreCase("NE_NAME")) {
							neName = exportValue.get(exportField) == null
									? ""
									: exportValue.get(exportField).toString();
						} else {
							// 导出自定义字段
							String key = mapField;
							// bug -83 发现有些导出字段为null值
							String value = "";
							if (entry.getValue() != null) {
								value = entry.getValue().toString();
							} else {
								logger.debug("发现导出字段{}为null值", key);
							}
							alarm.setField(key.toUpperCase(), value);
						}
					}
				}
			}
			
			// 告警时间要用任务当前监控时间，如果用最后一个event时间，则可能会有主键问突。
			alarm.setField("ALARM_TIME", DateTimeUtil
					.formatDateTime(monitorTask.getCurrMonitorTime()));
			alarm.setField("NE_SYS_ID", index);
			alarm.setField("NE_TYPE", dataSource.getNetType());
			alarm.setField("NE_LEVEL", dataSource.getNeLevel());
			alarm.setField("CITY_ID", cityId);
			// 频次和指标表达式两者中取最高级别
			// task_name + 主指标名称(第一个指标) + 告警级别
			String piName = monitorTask.getPiName() == null ? "" : monitorTask
					.getPiName();
			StringBuilder sb1 = new StringBuilder();
			alarm.setField("TITLE_TEXT",
					sb1.append(neName).append(";").append(piName).append(";")
							.append(MonitorConstant.levelMap.get(level))
							.toString());
			/*
			 * 告警内容: {网元名称} {主指标名称} {告警级别},在{监控周期数量}{监控周期单位}出现{出现次数}次,{指标条件},: {
			 * {数据时间} {指标值}[/{指标值}...], } G-幽谷神潭-2 TCH掉话率%(含切)严重告警,在5
			 * 小时内出现3次,TCH掉话率%(含切)>2.0, 并且 TCH占用成功次数>20.0, : 2013-02-14 10:00:00
			 * 4.1667/24.0,2013-02-15 10:00:00 4.6512/43.0,2013-02-16 10:00:00
			 * 4.4444/45.0。
			 */
			// 网元名称
			sb2.append(neName);
			// 主指标名称
			// sb2.append(" ").append(monitorTask.getPimaryName().trim());
			// 把每一个指标的名称都依次列出来
			sb2.append(" ");
			for (String indicatorName : lstIndicatorNames) {
				sb2.append(indicatorName.trim()).append("/");
			}
			if (lstIndicatorNames.size() > 0)
				sb2.deleteCharAt(sb2.length() - 1);
//			else {
//				if (StringUtils.isNotBlank(monitorTask.getPiName())) 
//					sb2.append(monitorTask.getPiName().trim());
//				else
//					sb2.append("主指标");
//			}

			// 告警级别
			sb2.append(" ").append(MonitorConstant.levelMap.get(level));
			// 分析规则 如: 在7 小时内出现3次
			sb2.append(",").append(
					monitorTask.getRuleDescription() == null ? "" : monitorTask
							.getRuleDescription());
			// sb2.append(",").append(monitorTask.getPimaryDescription().trim());
			// // 指标条件
			sb2.append(",:").append(eventStr).toString();
			
			alarm.setField("ALARM_TEXT", sb2.toString());
			alarm.setField("ALARM_STATE", "0");
			alarm.setField("ALARM_LEVEL", String.valueOf(level));
			alarm.setField("START_TIME", DateTimeUtil.formatDateTime(startTime));
			alarm.setField("END_TIME", DateTimeUtil.formatDateTime(EndTime));
			// 区分闭环和监控任
			alarm.setField("MONITOR_ID",
					String.valueOf(monitorTask.getTaskId()));
			alarm.setField("MONITOR_TASK_ID", monitorTask.getMonitorTaskId());
			alarm.setField("MONITOR_VALUE", monitorValue);
			// 任务 1=清除，0=触发, 2=驳回
			if (lastEvent.getLevel() == Event.INVALID_LEVEL) 
				alarm.setField("IS_CLEAR", "2");
			else
				alarm.setField("IS_CLEAR", monitorTask.getAlarmClear() + "");
			alarm.setField("CURR_MONITOR_TIME", DateTimeUtil
					.formatDateTime(monitorTask.getCurrMonitorTime()));
			alarm.setField("NE_NAME", neName);
			// Memo
			String memoStr = monitorTask.getMemo();
			if (StringUtils.isNotBlank(memoStr)) {
				String[] memoAry = StringUtils.splitPreserveAllTokens(memoStr,
						US);
				for (int i = 0; i < memoAry.length; i++) {
					if (alarm.getData().containsKey("C" + i))
						continue;

					alarm.setField("C" + i, memoAry[i]);
				}
			}

			// 设置告警ID
			alarm.setAlarmID(AlarmDAO.getInstance().getAlarmIDSeq(alarmConn));
			alarms.add(alarm);
		}

		logger.debug("频次规则处理完毕，满足条件的监控对象共{}个", alarmEvents.size());
		return alarms;
	}

	public Map<String, List<Event>> handleFrequencyRuleHandle(
			OccurTimes occurTimes, List<Date> eventDataTimes, Map<String, Date> nesysReplayTimeMap) throws Exception {
		// 频次规则中如果解析周期信息后的时间范围为空，则本次运算失败，输出日志告警。
		if (eventDataTimes == null || eventDataTimes.size() == 0) {
			logger.error("频次规则运算时间范围不能为空！");
			return null;
		}

		EventProvider eventProvider = new EventProvider(eventDataTimes,
				monitorTask, indicatorResult == null
						? null
						: indicatorResult.getEventBlockData());
		
		FrequencyRuleHandler handler = new FrequencyRuleHandler(occurTimes,
				eventProvider, nesysReplayTimeMap);
		Map<String, List<Event>> alarmableEvent = handler.handle();

		return alarmableEvent;
	}

	/**
	 * <pre>
	 * 规则处理
	 * 1、先处理频次规则
	 * 2、处理TOP-N规则
	 * 
	 * @param rule 当前监控任务对应的规则配置
	 * </pre>
	 */
	private boolean handleRule(Connection alarmConn, Connection taskConn, Rule rule, List<Date> eventDataTimes)
			throws Exception {
		// 时间为节假日不出来,
		// 频次规则中如果解析周期信息后的时间范围为空，则本次运算失败，输出日志告警。
		if (eventDataTimes == null || eventDataTimes.size() == 0) {
			logger.error("频次规则运算时间范围不能为空！");
			return false;
		}

		MonitorTaskStatusDAO mtsDao = MonitorTaskStatusDAO.getInstance();
		long taskId = monitorTask.getTaskId();
		Timestamp currMonitorTime = monitorTask.getCurrMonitorTime();

		Map<String, List<Event>> alarmableEvent;
		int eventNum = 0;
		String failCause = null;
		Map<String, Date> nesysReplayTimeMap = null;
		if(monitorTask.getAlarmClear() != FilterBuilder.ALARM_MONITOR_FLAG){
			nesysReplayTimeMap = RejectDAO.getInstance().getNeSysReplyTime(alarmConn, monitorTask.getMonitorTaskId(),
					monitorTask.getCurrMonitorTime());
		}
		// 频次规则运算
		try {
			mtsDao.updateMonitorTaskStatusForStartFrequency(taskConn, taskId,
					currMonitorTime);
			
			alarmableEvent = handleFrequencyRuleHandle(rule.getPeriodInfo()
					.getOccurTimes(), eventDataTimes, nesysReplayTimeMap);
			
			eventNum = alarmableEvent.size();
		} catch (Exception e) {
			failCause = e.getMessage();
			
			throw e;
		} finally {
			mtsDao.updateMonitorTaskStatusForEndFrequency(taskConn, taskId,
					currMonitorTime, eventNum, failCause);
		}
		
		// 待程序稳定后，如果日志中无“未到出告警时间或已经被处理”，则可以去掉本段。
		// 首次出告警时间（适用于结单和驳回）
		if((null != alarmableEvent)&&(monitorTask.getAlarmClear() != FilterBuilder.ALARM_MONITOR_FLAG)&&(null != nesysReplayTimeMap)){
			// 当前监控时间回退一个监控主周期
			OccurTimes monitorMainOccurTime = monitorTask.getRule().getPeriodInfo().getOccurTimes();
			int granNum = monitorMainOccurTime.getValue();
			TimeUnit gran = monitorMainOccurTime.getUnit();
			long tmpMoniterTime = DateGranularityUtil
					.backwardTimeTravel(monitorTask.getCurrMonitorTime(), gran.toString(), granNum).getTime();
			// 循环剔除不符合网元
			Iterator<Map.Entry<String, List<Event>>> it = alarmableEvent.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry<String, List<Event>> entry = it.next();
				String neSysId = entry.getKey();
				// 回单时间
				Date replayDate = nesysReplayTimeMap.get(neSysId);
				// 任务的监控主频次(比如“7天中任意4天满足要求”中的4)
				
				// 如果“当前监控时间回退一个监控主周期后”<=“回单时间”，就不出告警
				if((replayDate == null) || (tmpMoniterTime <= replayDate.getTime())){
					it.remove();
					logger.debug("未到出告警时间或已经被处理："+neSysId+";delStatus:"+alarmableEvent.containsKey(neSysId));
				}
			}
		}
		
		// TOP规则是对频次规则运算结果进行处理，不直接面向事件池
		try {
			mtsDao.updateMonitorTaskStatusForStartTopN(taskConn, taskId, currMonitorTime);
			
			TopRuleHandler topRuleHandler = new TopRuleHandler(rule.getTop(),
					alarmableEvent);
			alarmableEvent = topRuleHandler.handle();
			
			eventNum = alarmableEvent.size();
		} catch (Exception e) {
			failCause = e.getMessage();
			
			throw e;
		} finally {
			mtsDao.updateMonitorTaskStatusForEndTopN(taskConn, taskId, currMonitorTime,
					eventNum, failCause);
		}
		
		// 告警次数对应级别 取表达式和告警级别规则最小值（最高告警级别）
		try {
			mtsDao.updateMonitorTaskStatusForStartAlarmLevel(taskConn, taskId,
					currMonitorTime);
			LevelRuleHandler levelRuleHandler = new LevelRuleHandler(
					rule.getAlarmLevel(), alarmableEvent);
			alarmableEvent = levelRuleHandler.handle();
			
			eventNum = alarmableEvent.size();
		} catch (Exception e) {
			failCause = e.getMessage();
			
			throw e;
		} finally {
			mtsDao.updateMonitorTaskStatusForEndAlarmLevel(taskConn, taskId,
					currMonitorTime, eventNum, failCause);
		}
		
		// 事件数满足要求产生告警
		if (eventNum == 0) {
			logger.debug("任务{}，数据时间{}，产生告警记录数为0",
					new Object[]{monitorTask.getTaskId(), currMonitorTime});
			return true;
		}
		
		List<Alarm> alarmList = createAlarm(alarmConn, taskConn, alarmableEvent);
		if (alarmList == null || alarmList.isEmpty()) {
			logger.debug("任务{}，本次产生告警条数为0", taskId);
			return true;
		}
		
		try {
			mtsDao.updateMonitorTaskStatusForStartAlarmExport(taskConn, taskId,
					currMonitorTime);
			
			AlarmDAO dao = AlarmDAO.getInstance();
			int result = dao.save(alarmConn, alarmList,
					monitorTask.getKeyIndexType(), monitorTask.gettId(), Configuration.getBoolean(Configuration.ALARM_DETAIL_ENABLE));
			
			logger.debug("任务{}，本次提交入主表{}条", new Object[]{taskId, result});
		} catch (Exception e) {
			failCause = e.getMessage();
			
			throw e;
		} finally {
			mtsDao.updateMonitorTaskStatusForEndAlarmExport(taskConn, taskId,
					currMonitorTime, eventNum, failCause);
		}

		return true;
	}

	/**
	 * 处理规程，出现异常捕获异常
	 * 
	 * @param rule
	 * @return
	 */
	private boolean checkHandleRule(Connection alarmConn, Connection taskConn, Rule rule, List<Date> eventTimeList) {
		try {
			return handleRule(alarmConn, taskConn, rule, eventTimeList);
		} catch (Exception e) {
			logger.error(
					"任务{}，数据时间{}，规则处理异常",
					new Object[]{monitorTask.getTaskId(),
							monitorTask.getCurrMonitorTime(), e});
			return false;
		}
	}

}
