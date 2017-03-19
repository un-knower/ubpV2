package cn.uway.ubp.monitor.task;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.data.model.FieldIndexInfo;
import cn.uway.framework.data.model.GroupingArrayData;
import cn.uway.framework.util.DateTimeUtil;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.ubp.monitor.context.Configuration;
import cn.uway.ubp.monitor.context.DatasourceAccessLog;
import cn.uway.ubp.monitor.context.DbPoolManager;
import cn.uway.ubp.monitor.dao.MonitorTaskDAO;
import cn.uway.ubp.monitor.dao.MonitorTaskStatusDAO;
import cn.uway.ubp.monitor.dao.RejectDAO;
import cn.uway.ubp.monitor.data.BlockData;
import cn.uway.ubp.monitor.data.BlockDataProviderImpl;
import cn.uway.ubp.monitor.data.Busy;
import cn.uway.ubp.monitor.data.BusyImpl;
import cn.uway.ubp.monitor.data.GroupBlockData;
import cn.uway.ubp.monitor.event.Event;
import cn.uway.ubp.monitor.event.EventBlockData;
import cn.uway.ubp.monitor.event.LocalEventAccessor;
import cn.uway.ubp.monitor.indicator.ExpressionParam;
import cn.uway.ubp.monitor.indicator.IndicatorResult;
import cn.uway.ubp.monitor.indicator.IndicatorRunner;
import cn.uway.ubp.monitor.indicator.filter.FilterBuilder;
import cn.uway.ubp.monitor.indicator.filter.FilterHandler;
import cn.uway.ubp.monitor.indicator.filter.IFilter;
import cn.uway.ubp.monitor.rule.job.RuleJobFuture;
import cn.uway.ubp.monitor.rule.job.RuleWork;
import cn.uway.ubp.msg.ExpresstionException;
import cn.uway.ubp.msg.FilterException;
import cn.uway.ubp.msg.RuleException;
import cn.uway.util.DateGranularityUtil;
import cn.uway.util.MonitorUtil;
import cn.uway.util.MonitorUtil.MonitorPeriodTimeInfo;
import cn.uway.util.entity.AnalysisPeriod;
import cn.uway.util.entity.DataRange;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.Expression;
import cn.uway.util.entity.Filter;
import cn.uway.util.entity.Holiday;
import cn.uway.util.entity.Indicator;
import cn.uway.util.entity.MonitorPeriod;
import cn.uway.util.entity.MonitorTask;
import cn.uway.util.entity.OccurTimes;
import cn.uway.util.entity.Offset;
import cn.uway.util.entity.PeriodInfo;
import cn.uway.util.entity.Range;
import cn.uway.util.entity.Rule;
import cn.uway.util.entity.SubOccurTimes;

/**
 * <pre>
 * 监控任务执行线程
 * 1、判断监控任务是否需要运行
 * 2、执行指标表达式运算
 * 3、执行监控任务规则运算
 * 
 * @author Chris @ 2013-11-28
 * </pre>
 */
public class Job implements Comparable<Job> {

	/**
	 * 提交至job线程的监控任务
	 */
	private MonitorTask monitorTask;

	// 日志
	protected static final Logger logger = LoggerFactory.getLogger(Job.class);

	// 构造方法
	public Job(MonitorTask monitorTask) {
		this.monitorTask = monitorTask;
	}

	public MonitorTask getMonitorTask() {
		return monitorTask;
	}

	public void run() throws Exception {
		Connection taskConn = null;
		Connection alarmConn = null;
		try {
			Thread.currentThread().setName("监控任务" + monitorTask.getTaskId());

			logger.debug(
					"任务{}开始执行，当前监控时间{}",
					new Object[]{monitorTask.getTaskId(),
							monitorTask.getCurrMonitorTime()});

			// {准备数据
			// 当前任务的执行时间
			Timestamp currMonitorTime = monitorTask.getCurrMonitorTime();
			Timestamp currTaskRunTime = currMonitorTime;

			DataSource dataSource = monitorTask.getDataSource();

			Integer policy = null;
			Filter filter = monitorTask.getFilter();
			if (filter != null) {
				Holiday holiday = filter.getHoliday();
				if (holiday != null) {
					policy = holiday.getPolicy();
				}
			}

			Expression expression = monitorTask.getExpression();
			DataRange dataRange = expression.getDataRange();
			List<Range> rangeList = dataRange.getRangeList();

			Rule rule = monitorTask.getRule();
			PeriodInfo periodInfo = rule.getPeriodInfo();
			AnalysisPeriod analysisPeriod = periodInfo.getAnalysisPeriod();
			MonitorPeriod monitorPeriod = periodInfo.getMonitorPeriod();
			OccurTimes occurTimes = periodInfo.getOccurTimes();
			// 准备数据结束}

			// 每个监控周期的数量
			int nMonitorPeriodNum = monitorPeriod.getPeriodNum();
			// 监控周期单位
			String monitorPeriodUnit = monitorPeriod.getUnit().toString();
			// 数据范围
			Map<String, List<Offset>> rangeOffsets = MonitorUtil
					.offsets(rangeList);
			// 事件日期列表
			List<Date> eventTimeList = new LinkedList<Date>();

			IndicatorResult indicatorResult = null;
			int eventNum = 0;
			int eventNumSum = 0;
			String failCause = null;
			StringBuilder experience = new StringBuilder();
			/**
			 * <pre>
			 * 关于表达式运算过程跟踪内容的详细描述：
			 * 波动：
			 * 	内容：
			 * 		监控周期需要的时间点、加载的数据记录总和、经过的各层过滤器输入输出数量
			 * 	格式约定：
			 * 	<root>
			 * 		<range-list>
			 * 			<range name="a">
			 * 				<data-time time="timestamp">
			 * 					<load count="加载的数据记录总和" />
			 * 					<filter-list>
			 * 						<filter in="输入数量" out="输出数量">
			 * 							<![CDATA[过滤器内容]]>
			 * 						</filter>
			 * 						<filter in="输入数量" out="输出数量">
			 * 							<![CDATA[过滤器内容]]>
			 * 						</filter>
			 * 					</filter-list>
			 * 				</data-time>
			 * 			</range>
			 * 		</range-list>
			 * 		<expression event-number="产生事件数量" />
			 * 	</root>
			 * ==============================================
			 * 频次：
			 * 	内容：
			 * 		循环需要运算的时间点、（双频次：循环数据点、表达式运算、频次运算）、（单频次：表达式运算）
			 * 	格式约定：
			 * 	<root>
			 * 		<monitor-time-list>
			 * 			<!--双频次-->
			 * 			<monitor-time time="timestamp">
			 * 				<sub>
			 * 		 		<range-list>
			 * 						<range name="a">
			 * 							<data-time time="timestamp">
			 * 								<load count="加载的数据记录总和" />
			 * 								<filter-list>
			 * 									<filter in="输入数量" out="输出数量">
			 * 										<![CDATA[过滤器内容]]>
			 * 									</filter>
			 * 									<filter in="输入数量" out="输出数量">
			 * 										<![CDATA[过滤器内容]]>
			 * 									</filter>
			 * 								</filter-list>
			 * 									</data-time>
			 * 						</range>
			 * 					</range-list>
			 * 					<expression event-number="产生事件数量" />
			 * 				</sub>
			 * 			</monitor-time>
			 * 			<!--单频次-->
			 * 			<monitor-time time="timestamp">
			 * 			<range-list>
			 * 					<range name="a">
			 * 						<data-time time="timestamp">
			 * 							<load count="加载的数据记录总和" />
			 * 							<filter-list>
			 * 								<filter in="输入数量" out="输出数量">
			 * 									<![CDATA[过滤器内容]]>
			 * 								</filter>
			 * 								<filter in="输入数量" out="输出数量">
			 * 									<![CDATA[过滤器内容]]>
			 * 								</filter>
			 * 							</filter-list>
			 * 					</data-time>
			 * 					</range>
			 * 				</range-list>
			 * 				<expression event-number="产生事件数量" />
			 * 			</monitor-time>
			 * 		</monitor-time-list>
			 * 	</root>
			 * </pre>
			 */

			taskConn = DbPoolManager.getConnectionForTask();
			alarmConn = DbPoolManager.getConnectionForAlarm();
			try {
				MonitorTaskStatusDAO.getInstance()
						.updateMonitorTaskStatusForStartExpression(
								taskConn, monitorTask.getTaskId(), currMonitorTime);
				experience.append("<root>");

				boolean enableHoliday = Configuration
						.getBoolean(Configuration.HOLIDAY_ENABLE);

				// 非频次类运算
				if (rangeList.size() > 1) {
					// 最大的时间偏移数
					int maxOffsetTime = MonitorUtil
							.getMaxOffsetTimers(rangeOffsets);
					int minuOfMonitorPeriod = DateGranularityUtil
							.minutes(monitorPeriodUnit);
					// 分析个数 = 最大的偏移时间/每个监控周期单位时间粒度
					int nAnalsysisNum = maxOffsetTime / minuOfMonitorPeriod;
					List<MonitorPeriodTimeInfo> monitortimesInfo = MonitorUtil
							.getMonitorTimes(monitorPeriodUnit, nAnalsysisNum,
									monitorTask.getCurrMonitorTime(), rule,
									policy, dataSource.getGranularity()
											.toString(), enableHoliday,true,true);
					// 当本时间点不需要运算时，直接到下个时间点
					if(null == monitortimesInfo){
						// 将indicator跳转到下一个时间点
						markMonitorTime(taskConn);
						experience.append("</root>");
						return;
					}
					// 每个range(数据范围)对应的数据时间点
					Map<String, List<Date>> dateMap = new HashMap<String, List<Date>>();

					// 遍历range offset设置，找出对应的时间点
					for (Entry<String, List<Offset>> entry : rangeOffsets
							.entrySet()) {
						String aliasName = entry.getKey();
						List<Offset> offsetList = entry.getValue();
						List<Date> currRangeDateList = new LinkedList<Date>();
						dateMap.put(aliasName, currRangeDateList);

						for (Offset offset : offsetList) {
							int from = offset.getFrom();
							int to = offset.getTo();
							/*
							 * <pre> 以监控周期单位时间累加，依次找出时间点： <data-range> <range
							 * alias="B10"> <offset from="60" to="0"/> </range>
							 * </data-range> </pre>
							 */
							while (from > to) {
								int index = to / minuOfMonitorPeriod;
								to += minuOfMonitorPeriod;

								MonitorPeriodTimeInfo info = monitortimesInfo
										.get(index);
								currRangeDateList.addAll(0, info.dataTimes);
							}
						}
					}

					Map<String, GroupBlockData> rawData = buildGroupBlockData(alarmConn, taskConn, dataSource, dateMap, experience);
					
					if (rawData == null) {
						logger.debug("监控时间点{}包含无效数据(找不到缓存文件)，将跳过当前监控时间点",
								currMonitorTime);
						// 将indicator跳转到下一个时间点
						markMonitorTime(taskConn);
						return;
					}

					ExpressionParam expressionParam = new ExpressionParam();
					expressionParam.setDataTime(currTaskRunTime);
					eventTimeList.add(currTaskRunTime);
					expressionParam.setRawData(rawData);
					expressionParam.setDateRangeTimes(dateMap);
					expressionParam.setIndicatorList(expression
							.getIndicatorList());

					// 表达式计算
					indicatorResult = calcIndicator(alarmConn, dataSource, expressionParam, this.getMonitorTask().getAlarmClear() == FilterBuilder.ALARM_MONITOR_FLAG?true:false);

					experience.append("<expression event-number=\""
							+ indicatorResult.getEventBlockData().size()
							+ "\" />");
				} else {
					// 频次类运算

					// 按每个监控周期的单位时间个数，进行分组合并;
					List<MonitorPeriodTimeInfo> monitortimesInfo = MonitorUtil
							.getMonitorTimes(analysisPeriod.getUnit()
									.toString(), analysisPeriod.getPeriodNum(),
									monitorTask.getCurrMonitorTime(), rule,
									policy, dataSource.getGranularity()
											.toString(), enableHoliday, true,true);
					// 当本时间点不需要运算时，直接到下个时间点
					if(null == monitortimesInfo){
						// 将indicator跳转到下一个时间点
						markMonitorTime(taskConn);
						experience.append("</root>");
						return;
					}
					MonitorPeriodTimeInfo currGroupingMonitorPeriodInfo = null;
					int currGroupingPeriodNum = 0;
					// 当前任务按监控周期单位个数的时间数据信息列表，以监控周期时间点个数，进行分组
					List<MonitorPeriodTimeInfo> groupingMonitorPeriodInfos = new ArrayList<MonitorPeriodTimeInfo>();
					for (MonitorPeriodTimeInfo info : monitortimesInfo) {
						if (currGroupingMonitorPeriodInfo == null) {
							currGroupingMonitorPeriodInfo = info;
							currGroupingPeriodNum = 1;
						} else {
							// 数据时间点，升序排列
							currGroupingMonitorPeriodInfo.dataTimes.addAll(0,
									info.dataTimes);
							++currGroupingPeriodNum;
						}

						if (currGroupingPeriodNum >= nMonitorPeriodNum) {
							// 监控周期的时间点，以第一个有效的数据时间点
							currGroupingMonitorPeriodInfo.periodTime = currGroupingMonitorPeriodInfo.dataTimes
									.get(0);
							// 监控周期时间，升序排列
							groupingMonitorPeriodInfos.add(0,
									currGroupingMonitorPeriodInfo);

							currGroupingMonitorPeriodInfo = null;
							currGroupingPeriodNum = 0;
						}
					}

					long lastEventTime = getLastEventTime(dataSource,
							getMonitorTask());
					String alisName = rangeOffsets.entrySet().iterator().next()
							.getKey();
					boolean bMissionFiles = false;
					experience.append("<monitor-time-list>");

					for (MonitorPeriodTimeInfo monitorPeriodInfo : groupingMonitorPeriodInfos) {
						experience
								.append("<monitor-time time=\""
										+ DateTimeUtil
												.formatDateTime(monitorPeriodInfo.periodTime)
										+ "\">");

						eventTimeList.add(monitorPeriodInfo.periodTime);
						// 如果最后一次运算的监控周期时间大于或等于当前监控周期的时间，则无须运算，直接从本地缓存中读取event
						// <br/>
						if (monitorPeriodInfo.canSkip&&lastEventTime > 0
								&& (monitorPeriodInfo.periodTime.getTime() <= lastEventTime))
							continue;

						// 要对双频次每个监控周期作出频次运算 FIXME 这里有问题，occurTimes为null，配置上允许为空
						SubOccurTimes subOccurTimes = occurTimes
								.getSubOccurTimes();
						if (subOccurTimes != null
								&& subOccurTimes.getValue() > 0) {
							experience.append("<sub>");
							indicatorResult = caclMonitorPeriodRule(alarmConn, taskConn, 
									lastEventTime,monitorPeriodInfo, alisName, subOccurTimes
											.getUnit().toString(), dataSource
											.getGranularity().toString(),
									expression.getIndicatorList(), dataSource,
									subOccurTimes, experience);

							experience.append("</sub>");
							if (indicatorResult == null || indicatorResult.getEventBlockData().isEmpty()) {
								bMissionFiles = true;
								// 如果文件缺失，忽略当前监控周期
								continue;
							}
							eventNumSum = indicatorResult.getEventBlockData()
									.size();
						} else {
							Map<String, List<Date>> dateMap = new HashMap<String, List<Date>>();
							dateMap.put(alisName, monitorPeriodInfo.dataTimes);
							Map<String, GroupBlockData> rawData = buildGroupBlockData(alarmConn, taskConn, dataSource, dateMap, experience);
							
							if (rawData == null) {
								bMissionFiles = true;
								// 如果文件缺失，忽略当前监控周期
								continue;
							}

							ExpressionParam expressionParam = new ExpressionParam();
							expressionParam
									.setDataTime(monitorPeriodInfo.periodTime);
							expressionParam.setRawData(rawData);
							expressionParam.setDateRangeTimes(dateMap);
							expressionParam.setIndicatorList(expression
									.getIndicatorList());

							// 表达式计算
							indicatorResult = calcIndicator(alarmConn, dataSource,
									expressionParam, this.getMonitorTask().getAlarmClear() == FilterBuilder.ALARM_MONITOR_FLAG?true:false);
							// 添加表达式运算中的详细信息
							int tmpEvent = indicatorResult.getEventBlockData()
									.size();
							experience.append("<expression event-number=\""
									+ tmpEvent + "\" />");
							experience.append("<index-list>");
							if (tmpEvent > 0) {
								experience.append("<![CDATA[");
								Map<String, Event> dataMap = indicatorResult
										.getEventBlockData().getEvents();
								int flag = 1;
								for (String key : dataMap.keySet()) {
									if (flag % 100 == 0)
										experience.append("\n");
									experience.append(key).append(",");
									// 在粒度为小时的时候，为了不影响性能每个小时的输出记录最多记录80000个
									// 超过8万个记录数据库CLOB字段会记录为 <Value Error>
									if (flag++ > 80000)
										break;
								}
								experience.append("]]>");
							}
							experience.append("</index-list>");
							eventNumSum += tmpEvent;

						}

						experience.append("</monitor-time>");
					}

					experience.append("</monitor-time-list>");

					if (indicatorResult == null || indicatorResult.getEventBlockData().isEmpty()) {
						if (bMissionFiles) {
							logger.debug(
									"监控时间点{}包含无效的数据时间点(找不到缓存文件)，将跳过当前监控时间点",
									currMonitorTime);
						} else {
							logger.debug("监控时间点{}不在监控时间点内，下一个周期继续运算",
									currMonitorTime);
							failCause = "监控时间点" + currMonitorTime
									+ "不在监控时间点内，下一个周期继续运算;lastEventTime:"+lastEventTime;
						}
						// 将indicator跳转到下一个时间点
						markMonitorTime(taskConn);
						experience.append("</root>");
						return;
					}

				}

				// 新增逻辑 在频次运算时，最终输出的eventNum 应是每个监控点eventNum的和
				if (rangeList.size() > 1) {
					eventNum = indicatorResult.getEventBlockData().size();
				} else {
					eventNum = eventNumSum;
				}
				experience.append("</root>");
			} catch (Exception e) {
				failCause = e.getMessage();
				throw e;
			} finally {
				MonitorTaskStatusDAO.getInstance()
						.updateMonitorTaskStatusForEndExpression(
								taskConn, monitorTask.getTaskId(), currMonitorTime,
								experience.toString(), failCause, eventNum);
			}

			calRule(alarmConn, taskConn, indicatorResult, eventTimeList);
		} catch (FilterException e) {
			// FIXME 这里抛出的异常不准确！！！暂时先取消设置任务为无效状态
			// 数据加载过滤失败，更新任务状态表状态(0表示失败, 1表示成功),并暂停任务
			// MonitorTaskDAO.getInstance().setTaskNotValid(monitorTask.getTaskId());
			logger.debug("数据加载过滤失败,任务更改为无效状态");
			throw e;
		} catch (ExpresstionException e) {
			// FIXME 这里抛出的异常不准确！！！暂时先取消设置任务为无效状态
			// 表达式失败，更新任务状态表状态(0表示失败, 1表示成功),并暂停任务
			// MonitorTaskDAO.getInstance().setTaskNotValid(monitorTask.getTaskId());
			logger.debug("表达式计算失败,任务更改为无效状态");
			throw e;
		} catch (RuleException e) {
			// FIXME 这里抛出的异常不准确！！！暂时先取消设置任务为无效状态
			// 任务执行失败，更新任务状态表状态(0表示失败, 1表示成功),并暂停任务
			// MonitorTaskDAO.getInstance().setTaskNotValid(monitorTask.getTaskId());
			logger.debug("规则运算失败,任务更改为无效状态");
			throw e;
		} catch (Exception e) {
			throw e;
		}finally{
			DatabaseUtil.close(taskConn);
			DatabaseUtil.close(alarmConn);
		}
	}

	/**
	 * 对每个监控周期进行子频次运算
	 * 
	 * @param monitorPeriodinfo
	 * @param alisName
	 * @param monitorPeriodOccurTimeUnit
	 * @param dsGran
	 * @param indicatorList
	 * @param dataSource
	 * @param occurTimes
	 * @return
	 * @throws Exception
	 */
	private IndicatorResult caclMonitorPeriodRule(Connection alarmConn, Connection taskConn, 
			long lastEventTime,MonitorPeriodTimeInfo monitorPeriodinfo, String alisName,
			String monitorPeriodOccurTimeUnit, String dsGran,
			List<Indicator> indicatorList, DataSource dataSource,
			OccurTimes occurTimes, StringBuilder experience) throws Exception {
		if (monitorPeriodOccurTimeUnit == null
				|| monitorPeriodOccurTimeUnit.length() < 1)
			monitorPeriodOccurTimeUnit = dsGran;
		List<Date> analysisMonitorPeriodEventTimeList = new LinkedList<Date>();
		IndicatorResult indicatorResult = null;
		// Date beginTime = new Date();

		// 当前数据分组的数据时间点
		List<Date> currDataGroupList = new LinkedList<Date>();
		// 上一个数据分组
		Date prvGroupDate = null;
		// 在监控周期monitorPeriodinfo.dataTimes中的数据时间是顺序存放的
		Iterator<Date> iter = monitorPeriodinfo.dataTimes.iterator();
		
		while (iter.hasNext() || currDataGroupList.size() > 0) {
			Date dsDate = null;
			if (iter.hasNext()) {
				dsDate = iter.next();
				
				// 按监控周期出现次数的单位取整(监控周期是"周"，如数据粒度是"小时"，则数据量为(7*24)，出现次数指定的是"天"，<br/>
				// 那么就应该将一天的数据作为一个分组(24小时数据)运算一次，而不是每小时运算一次
				Date currGroupDate = DateGranularityUtil.truncate(dsDate,
						monitorPeriodOccurTimeUnit);
				if (prvGroupDate == null)
					prvGroupDate = currGroupDate;
				
				if (prvGroupDate.compareTo(currGroupDate) == 0) {
					currDataGroupList.add(dsDate);
					continue;
				} else {
					prvGroupDate = currGroupDate;
				}
			}
			// 当前监控周期用于计算频次的event分析时间
			analysisMonitorPeriodEventTimeList.add(currDataGroupList.get(0));
			
			// 已处理的时间点，不需要再次计算
			if(currDataGroupList.get(0).getTime() > lastEventTime){
				
				// 当数据按监控周期出现次数单位所属分组改变时，或最后一条记录，则开始运算:
				// 计算当前数据分组
				Map<String, List<Date>> dateMap = new HashMap<String, List<Date>>();
				dateMap.put(alisName, currDataGroupList);
				Map<String, GroupBlockData> rawData = buildGroupBlockData(alarmConn, taskConn, dataSource, dateMap, experience);
				
				if (rawData == null)
					return null;
				
				ExpressionParam expressionParam = new ExpressionParam();
				expressionParam.setDataTime(currDataGroupList.get(0));
				expressionParam.setRawData(rawData);
				expressionParam.setDateRangeTimes(dateMap);
				expressionParam.setIndicatorList(indicatorList);
				
				// 表达式计算
				indicatorResult = calcIndicator(alarmConn, dataSource, expressionParam,  this.getMonitorTask().getAlarmClear() == FilterBuilder.ALARM_MONITOR_FLAG ?true:false);
				// calcIndicator(dataSource, expressionParam);
			}
			
			// 计算完后，要将当前数据分组的数据时间点清空
			currDataGroupList.clear();
			
			// 将当前时间加入到currDataGroupList中；
			if (dsDate != null)
				currDataGroupList.add(dsDate);
		}

		// 对监控周期进行频次运算
		RuleWork ruleWork = new RuleWork(monitorTask, indicatorResult);
		// 回单时间
		Map<String, Date> nesysReplayTimeMap = null;
		if(this.getMonitorTask().getAlarmClear() != FilterBuilder.ALARM_MONITOR_FLAG){
			nesysReplayTimeMap =RejectDAO.getInstance().getNeSysReplyTime(alarmConn, monitorTask.getMonitorTaskId(),
					monitorTask.getCurrMonitorTime());
		}
		Map<String, List<Event>> handleResult = ruleWork
				.handleFrequencyRuleHandle(occurTimes,
						analysisMonitorPeriodEventTimeList, nesysReplayTimeMap);
		// rule.getMonitorOccurTimes(), rule.isMonitorContinues());
		if (handleResult == null || handleResult.isEmpty()) {
			IndicatorResult result = new IndicatorResult();
			result.setEventBlockData(new EventBlockData());
			// result.setMonitorTaskStatus(status);
			// 开始进行指标表达式计算时间
			// status.setStartExpressionDate(beginTime);
			// 表达式运算状态(0表示失败, 1表示成功)
			// status.setCalExpressionStatus(1);
			// 失败原因
			// status.setCalExpressionCause(null);
			// 事件个数
			// status.setEventNum(0);
			// 完成指标表达式计算时间s
			// status.setEndExpressionDate(new Date());

			return result;
		}

		// 事件信息块
		EventBlockData eventBlockData = new EventBlockData();
		// 数据时间
		eventBlockData.setDataTime(monitorPeriodinfo.periodTime);

		Iterator<Entry<String, List<Event>>> iterEvents = handleResult
				.entrySet().iterator();
		while (iterEvents.hasNext()) {
			Entry<String, List<Event>> entry = iterEvents.next();
			List<Event> subEventList = entry.getValue();

			if (subEventList == null || subEventList.size() < 1)
				continue;

			/*if (entry.getKey().equalsIgnoreCase("1100030031272200")) {
				assert(false);
			}*/
			
			// 创建event事件
			Event ev = new Event();
			// 事件数据的group-key、value
			ev.setIndexKey(subEventList.get(0).getIndexKey());
			ev.setIndexValues(entry.getKey());

			// 子指标值(将每个时间点的指标值，合并)
			Map<Date, Map<String, Object>> indicatorValues = new HashMap<Date, Map<String, Object>>();
			for (Event subEv : subEventList) {
				Map<Date, Map<String, Object>> subIndicatorValues = subEv
						.getIndicatorValues();
				if (subIndicatorValues != null && subIndicatorValues.size() > 0) {
					indicatorValues.putAll(subIndicatorValues);
				} else {
					indicatorValues.put(subEv.getDataTime(), new HashMap<String, Object>());
				}
			}
			ev.setIndicatorValues(indicatorValues);
			// 事件时间
			ev.setDataTime(monitorPeriodinfo.periodTime);
			// 告警极别 (驳回告警级别和子Event级别一样)
			ev.setLevel(subEventList.get(0).getLevel());
			// 导出字段
			ev.setExportFieldsValue(subEventList.get(0).getExportFieldsValue());

			// 将新生成的event加入到event Blcok中
			eventBlockData.setEvent(ev);
		}

		// 保存eventblock
		if (monitorPeriodinfo.isWhole&&!storeEvent(monitorTask.getTaskId(), dataSource, eventBlockData)) {
			// 系列化失败，时间不往前累加
			logger.error(
					"任务{},数据时间{}的事件序列化失败",
					new Object[]{monitorTask.getTaskId(),
							dataSource.getDataTime()});
			throw new Exception("taskId=" + monitorTask.getTaskId()
					+ ",dataTime=" + dataSource.getDataTime() + "事件序列化失败.");
		}

		IndicatorResult result = new IndicatorResult();
		result.setEventBlockData(eventBlockData);
		// result.setMonitorTaskStatus(status);
		// 开始进行指标表达式计算时间
		// status.setStartExpressionDate(beginTime);
		// 表达式运算状态(0表示失败, 1表示成功)
		// status.setCalExpressionStatus(1);
		// 失败原因
		// status.setCalExpressionCause(null);
		// 事件个数
		// status.setEventNum(eventBlockData.size());
		// 完成指标表达式计算时间s
		// status.setEndExpressionDate(new Date());

		return result;
	}
	
	/**
	 * 计算表达式，并返回计算结果
	 * @param dataSource		数据源
	 * @param expressionParam	表达式
	 * @param bCreateAlarmOnlyFormularInTrueCase 是否只当表达式计算结果是true的情况下，才产生告警
	 * @return
	 * @throws Exception
	 */
	private IndicatorResult calcIndicator(Connection alarmConn, DataSource dataSource,
			ExpressionParam expressionParam, boolean bCreateAlarmOnlyFormularInTrueCase) throws Exception {
		IndicatorRunner indicatorRunner = new IndicatorRunner();

		Map<String, Date> nesysReplayTimeMap =  null;
		if (!bCreateAlarmOnlyFormularInTrueCase) {
			nesysReplayTimeMap = RejectDAO.getInstance().getNeSysReplyTime(alarmConn, monitorTask.getMonitorTaskId(),
					monitorTask.getCurrMonitorTime());
		}
		IndicatorResult indicatorResult = indicatorRunner
				.execute(expressionParam, bCreateAlarmOnlyFormularInTrueCase, nesysReplayTimeMap);
		logger.debug(
				"任务时间{}，数据时间{}，指标计算产生事件数量{}",
				new Object[]{monitorTask.getCurrMonitorTime(),
						expressionParam.getDataTime(),
						indicatorResult.getEventBlockData().size()});

		if (indicatorResult.getStatus() == 0) {
			// 公式计算错误，时间不往前累加
			throw new ExpresstionException("表达式计算异常,状态:"
					+ indicatorResult.getStatus() + ",错误原因:"
					+ indicatorResult.getCause());
		}

		if (!storeEvent(monitorTask.getTaskId(), dataSource,
				indicatorResult.getEventBlockData())) {
			// 系列化失败，时间不往前累加
			logger.error(
					"任务{}，数据时间{}事件序列化失败",
					new Object[]{monitorTask.getTaskId(),
							dataSource.getDataTime()});
			throw new Exception("任务" + monitorTask.getTaskId() + ",数据时间"
					+ dataSource.getDataTime() + "事件序列化失败.");
		}

		return indicatorResult;
	}

	/**
	 * @param indicatorResult
	 * @return
	 * @throws Exception
	 */
	private void calRule(Connection alarmConn, Connection taskConn, IndicatorResult indicatorResult,
			List<Date> eventTimeList) throws Exception {
		RuleWork ruleWork = new RuleWork(monitorTask, indicatorResult);
		RuleJobFuture ruleJobFuture = ruleWork.call(alarmConn, taskConn, eventTimeList);
		if (ruleJobFuture.getStatus() == 0)
			throw new RuleException(ruleJobFuture.getCause());

		// 执行完事件本地序列化、告警入库后更新任务的监控时间
		markMonitorTime(taskConn);
	}

	private void markMonitorTime(Connection taskConn) throws Exception {
		boolean enableHoliday = Configuration
				.getBoolean(Configuration.HOLIDAY_ENABLE);
		Date nextMonitorTime = MonitorUtil.nextMonitorTime(monitorTask,
				enableHoliday);
		boolean flag = MonitorTaskDAO.getInstance().markNextMonitorTime(
				taskConn, monitorTask.getTaskId(), nextMonitorTime);
		if (!flag)
			throw new Exception("任务" + monitorTask.getTaskId() + "更新监控时间失败");

		monitorTask
				.setCurrMonitorTime(new Timestamp(nextMonitorTime.getTime()));
		logger.debug("更新当前任务监控时间{}", nextMonitorTime);
	}

	/**
	 * 时间本地序列化<br>
	 * 
	 * @param taskId
	 * @param dataSource
	 * @param blockData
	 * @return boolean 是否序列化成功
	 */
	private boolean storeEvent(Long taskId, DataSource dataSource,
			EventBlockData blockData) {
		// 没有Event 直接返回
		if (blockData == null || blockData.size() == 0)
			return true;

		String path = BlockDataProviderImpl.getInstance().getPath(dataSource);
		LocalEventAccessor localEventAccessor = LocalEventAccessor
				.getInstance();
		try {
			localEventAccessor.lock();
			localEventAccessor.apply(path, taskId);
			localEventAccessor.add(blockData);
			return true;
		} catch (Exception e) {
			logger.error("存储事件文件异常", e);
			return false;
		} finally {
			localEventAccessor.unLock();
		}
	}

	private long getLastEventTime(DataSource dataSource, MonitorTask monitorTask) {
		String path = BlockDataProviderImpl.getInstance().getPath(dataSource);
		LocalEventAccessor localEventAccessor = LocalEventAccessor
				.getInstance();
		try {
			localEventAccessor.lock();
			localEventAccessor.apply(path, monitorTask.getTaskId());
			return localEventAccessor.getLastEventTimer();
		} catch (Exception e) {
			logger.error("读取事件文件异常", e);
		} finally {
			localEventAccessor.unLock();
		}

		return 0;
	}

	/**
	 * 根据DataRange为时间区间组合过滤后的数据，以索引为键值的数据
	 * 
	 * @param rangeList
	 * @return
	 * @throws Exception
	 */
	private Map<String, GroupBlockData> buildGroupBlockData(
			Connection alarmConn, Connection taskConn, DataSource dataSource, Map<String, List<Date>> dateMap,
			StringBuilder experience) throws Exception {
		try {
			experience.append("<range-list>");

			Map<String, GroupBlockData> rawData = new HashMap<String, GroupBlockData>();
			Set<String> alias = dateMap.keySet();
			Iterator<String> iterator = alias.iterator();
			// TODO 忙时改为在加载任务的时候解析
			Busy busy = new BusyImpl(monitorTask.getFilterContent());
			List<IFilter> filterList = FilterBuilder.buildFilters(alarmConn, taskConn, monitorTask,
					dataSource, dateMap);
			while (iterator.hasNext()) {
				String aliasName = iterator.next();
				GroupBlockData groupBlockData = new GroupBlockData();
				rawData.put(aliasName, groupBlockData);
				Map<String, List<GroupingArrayData>> groupingArrayDataMap = new HashMap<>();
				groupBlockData.setGroupingArrayDatas(groupingArrayDataMap);
				// 获取字段列
				List<String> columns = MonitorTaskDAO.getInstance()
						.getTaskField(taskConn, monitorTask.getTaskId());

				// // debug
				// List<String> columns = new ArrayList<String>();
				// columns.add("INTRAFREQ_MR_NUM_D");
				// columns.add("START_TIME");
				// columns.add("NE_CELL_ID");
				// columns.add("CELL_NAME");
				// columns.add("CITY_ID");
				// columns.add("DOWNCOVERGOOD_NUM_D");

				List<Date> dateList = dateMap.get(aliasName);
				BlockData blockData = null;

				experience.append("<range name=\"" + aliasName + "\">");

				logger.debug(
						"任务{}，任务时间点{}，数据集合名称{}，数据时间点个数{}",
						new Object[]{monitorTask.getTaskId(),
								monitorTask.getCurrMonitorTime(), aliasName,
								dateList.size()});
				boolean bMissAllFile = true;
				for (Date date : dateList) {
					experience.append("<data-time time=\""
							+ DateTimeUtil.formatDateTime(date) + "\">");

					// 忙时过滤时间
					busy.setDate(date);
					// C网忙时省市特殊处理
					if (busy.onBusy(monitorTask.getCityId()))
						continue;

					blockData = BlockDataProviderImpl.getInstance().load(
							dataSource, monitorTask.getCityId(), columns, date,
							busy);

					// 数据没加载成功，跳过
					if (blockData == null) {
						logger.warn("任务{}数据文件缺失，任务时间点{}，数据时间点{}",
								new Object[]{monitorTask.getTaskId(),
										monitorTask.getCurrMonitorTime(), date});
						continue;
					}

					experience.append("<load count=\""
							+ blockData.getGroupingArrayDatas().size()
							+ "\" />");

					bMissAllFile = false;
					// throw new NullPointerException("数据源dataSourceId=" +
					// datasourceInfo.getId() + ";时间:" + date + " 不存在！");
					// 记录数据最后访问时间
					DatasourceAccessLog.logAccess(dataSource.getId(), date);
					// FilterHandler调用
					FilterHandler filterHandler = new FilterHandler(filterList,
							blockData, experience);
					
					BlockData blockDataFilter = filterHandler.handle(taskConn);
					//BlockData blockDataFilter = blockData;
					logger.debug(
							"任务{}，任务时间点{}，数据时间点{}，过滤前的记录数{}，过滤后的记录数{}",
							new Object[]{
									monitorTask.getTaskId(),
									monitorTask.getCurrMonitorTime(),
									date,
									blockData.getGroupingArrayDatas().size(),
									blockDataFilter.getGroupingArrayDatas()
											.size()});

					// 监控字段,注意:监控字段不是数据源的索引字段
					String monitorField = monitorTask.getMonitorField();
					String dsIndexKey = blockDataFilter.getIndexKey();
					FieldIndexInfo monitorFieldInfo = null;
					// 指定了监控字段的，数据计算应该以监控字段的名称分组，而不是数据源索引字段分组
					if (monitorField != null && monitorField.length() > 0
							&& !monitorField.equals(dsIndexKey)) {
						groupBlockData.setIndexKey(monitorField);
						// 找到监控字段的字段信息
						monitorFieldInfo = blockDataFilter.getMetaInfo()
								.getFileIndexInfo(monitorField.toLowerCase());
						if (monitorFieldInfo == null) {
							logger.error("任务{}，在当前数据源中，找不到监控字段{}",
									new Object[]{monitorTask.getTaskId(),
											monitorField});

							throw new Exception("任务" + monitorTask.getTaskId()
									+ ",在当前数据源中,找不到监控字段" + monitorField);
						}
					} else {
						groupBlockData.setIndexKey(dsIndexKey);
					}
					// 设置最后一个导出字段信息
					groupBlockData.setExportFieldsKey(blockDataFilter
							.getExportFieldsKey());
					// 设置最后一个字段属性表信息
					groupBlockData.setMetaInfo(blockDataFilter.getMetaInfo());

					// 循环每一条被过滤后的数据记录
					for (Entry<String, GroupingArrayData> entry : blockDataFilter
							.getGroupingArrayDatas().entrySet()) {
						String indexVaule = "";
						GroupingArrayData record = entry.getValue();

						if (monitorFieldInfo != null) {
							// 得到索引字段对应的值
							Object indexObjValue = record
									.getPropertyValue(monitorFieldInfo);
							if (indexObjValue == null)
								continue;

							// 对于浮点型的取整作分组，一般来讲，监控索引字段不能为浮点数，浮点数的直接比较是错误的
							if (indexObjValue instanceof Number) {
								Long val = ((Number) indexObjValue).longValue();
								indexVaule = val.toString();
							} else
								indexVaule = indexObjValue.toString();
						} else {
							indexVaule = entry.getKey();
						}

						// 将数据分组存放到groupingArrayDataList中
						List<GroupingArrayData> groupingArrayDataList = groupingArrayDataMap
								.get(indexVaule);
						if (groupingArrayDataList == null) {
							groupingArrayDataList = new ArrayList<GroupingArrayData>();
							groupingArrayDataMap.put(indexVaule,
									groupingArrayDataList);
						}
						groupingArrayDataList.add(record);
					}

					experience.append("</data-time>");
				}

				experience.append("</range>");

				// 如果一个数据分组内，一个时间点的数据都没加载成功，则本次加载失败，直接返回null,
				if (bMissAllFile)
					return null;
			}

			experience.append("</range-list>");

			return rawData;
		} catch (Exception e) {
			throw new Exception("获取序列化数据失败：" + e.getMessage(), e);
		}
	}

	@Override
	public int compareTo(Job job) {
		Short selfAlarmLevel = monitorTask.getHightAlarmLevel();
		Short otherAlarmLevel = job.getMonitorTask().getHightAlarmLevel();
		return selfAlarmLevel.compareTo(otherAlarmLevel);
	}

	// for test
	/*
	 * public static void main(String[] args) throws Exception { Calendar
	 * calendar = Calendar.getInstance(); calendar.set(Calendar.MONTH, 2);
	 * calendar.set(Calendar.DAY_OF_MONTH, 3);
	 * calendar.set(Calendar.HOUR_OF_DAY, 15); calendar.set(Calendar.MINUTE, 0);
	 * calendar.set(Calendar.SECOND, 0); calendar.set(Calendar.MILLISECOND, 0);
	 * long mills = calendar.getTimeInMillis();
	 * 
	 * DataSource ds = new DataSource(100, "DAY", new Timestamp(mills) , "8",
	 * "2", "", 300, "START_TIME", "date", "DS_CHECK_NE_PIECE_W", 0);
	 * 
	 * Map<String, List<Date>> dateMap = new HashMap<>(); ArrayList<Date> list =
	 * new ArrayList<Date>(); list.add(calendar.getTime()); dateMap.put("ca1",
	 * list);
	 * 
	 * StringBuilder sb = new StringBuilder();
	 * 
	 * List<Indicator> indicatorList = new ArrayList<>(); Indicator indicator =
	 * new Indicator(); indicator.setAlarmLevel((short) 2); //
	 * indicator.setContent(
	 * "(as(SUM(NVL(A1.IS_NULL_LENGTH_LONGITUDE,0)+NVL(A1.IS_NULL_LONGITUDE,0)) ,\"A1.经度为空和经度字符位数小于8位的数量\")  >= 0 AND as(SUM(A1.IS_NULL_ANT_VENDOR),\"A1.天线厂家为空数量\")  >= 0)"
	 * ); // indicator.setContent(
	 * "(as(SUM(NVL(A1.IS_NULL_LENGTH_LONGITUDE,0)+NVL(A1.IS_NULL_LONGITUDE,0)) ,\"A1.经度为空和经度字符位数小于8位的数量\") >= 0)"
	 * ); //
	 * indicator.setContent("(as(SUM(A1.IS_NULL_ANT_VENDOR),\"A1.天线厂家为空数量\")  >= 0)"
	 * ); // indicator.setContent(
	 * "  ( as(ROUND(DECODE(SUM(CA1.INTRAFREQ_MR_NUM_D),0,100,SUM(CA1.DOWNCOVERGOOD_NUM_D)/SUM(CA1.INTRAFREQ_MR_NUM_D)*100),4),\"CA1.周期性下行良好覆盖率\")  >= 70 ) "
	 * ); // indicator.setContent(
	 * "ex_or(as(CA1.INTRAFREQ_MR_NUM_D,\"指标1\")>=14740 or as(CA1.DOWNCOVERGOOD_NUM_D,\"指标2\")>=1226)"
	 * ); indicator.setContent(
	 * "as(CA1.INTRAFREQ_MR_NUM_D,\"指标1\")>=1474 or as(CA1.DOWNCOVERGOOD_NUM_D,\"指标2\")>=1226"
	 * ); indicatorList.add(indicator);
	 * 
	 * String filterContent = "<filter xmlns=\"http://www.w3school.com.cn\">" +
	 * "<include field=\"NE_CELL_ID\">2010100065059022</include>" // +
	 * "<includeSQL field=\"NE_CELL_ID\" connection-id=\"4\"><![CDATA[   select ne_cell_id from"
	 * // +
	 * "                          cfg_piececellrelat b where b.ne_piece_id in (select id from cfg_piece"
	 * // + "             where BITAND(USE_TYPE, 2) = 2" // +
	 * "             and replace(regexp_substr(scn_ty_id5 || '@' || scn_ty_id4 || '@' ||"
	 * // + "             scn_ty_id3 || '@' || scn_ty_id2 || '@' || scn_ty_id1,"
	 * // + "             '@+[0-9]+@+|@@@@[0-9]+|[0-9]+@', 1), '@', '') " // +
	 * "             in(select scn_ty_id from cfg_mt_scene_type   " // +
	 * "             start with scn_ty_id =161234" // +
	 * "             connect by  parent_id  = prior scn_ty_id)  ) and flag=2  ]]> </includeSQL>"
	 * // + "<holiday policy=\"0\" strategy=\"0\"/>" + "</filter>";
	 * 
	 * Document doc = DocumentHelper.parseText(filterContent); Element element =
	 * doc.getRootElement(); Filter filter = Resolver.resolveFilter(element,
	 * null);
	 * 
	 * MonitorTask task = new MonitorTask(); task.setTaskId(1665);
	 * task.setCityId(0); task.setFilterContent(filterContent);
	 * task.setCurrMonitorTime(new Timestamp(mills));
	 * task.setMonitorField("NE_CELL_ID"); task.setFilter(filter);
	 * 
	 * Job job = new Job(task); Map<String, GroupBlockData> rawData =
	 * job.buildGroupBlockData(ds, dateMap, sb);
	 * 
	 * ExpressionParam expressionParam = new ExpressionParam();
	 * expressionParam.setDataTime(new Date(mills));
	 * expressionParam.setRawData(rawData);
	 * expressionParam.setDateRangeTimes(dateMap);
	 * expressionParam.setIndicatorList(indicatorList);
	 * 
	 * IndicatorRunner indicatorRunner = new IndicatorRunner(); IndicatorResult
	 * indicatorResult = indicatorRunner.execute(expressionParam);
	 * 
	 * EventBlockData event = indicatorResult.getEventBlockData();
	 * System.out.println("event size:" + event.size()); for (Event e :
	 * event.getEvents().values()) { System.out.println("Data Time:" +
	 * e.getDataTime()); System.out.println("Level:" + e.getLevel());
	 * System.out.println("Index Key:" + e.getIndexKey());
	 * System.out.println("Index Value:" + e.getIndexValues());
	 * 
	 * System.out.println("==============export=============="); for
	 * (Entry<String, Object> entry : e.getExportFieldsValue().entrySet()) {
	 * System.out.println(entry.getKey() + ":" + entry.getValue()); }
	 * 
	 * System.out.println("==============indicator=============="); for
	 * (Entry<Date, Map<String, Object>> entry :
	 * e.getIndicatorValues().entrySet()) { Date key = entry.getKey();
	 * System.out.println("\t" + key);
	 * 
	 * Map<String, Object> value = entry.getValue(); for (Entry<String, Object>
	 * _entry : value.entrySet()) { System.out.println("\t" + _entry.getKey() +
	 * ":" + _entry.getValue()); }
	 * 
	 * System.out.println("\t--------"); } } }
	 */

}
