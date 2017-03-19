package cn.uway.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import cn.uway.util.entity.AssignTime;
import cn.uway.util.entity.Filter;
import cn.uway.util.entity.Holiday;
import cn.uway.util.entity.MonitorTask;
import cn.uway.util.entity.Offset;
import cn.uway.util.entity.Range;
import cn.uway.util.entity.Rule;
import cn.uway.util.enums.TimeUnit;

/**
 * <pre>
 * 监控规则工具类
 * 主要功能:
 * 1、处理监控规则中的周期信息，主要是指定時間
 * 2、处理节假日策略
 * </pre>
 * 
 * @author Chris @ 2013-11-12
 */
public class MonitorUtil {

	/**
	 * 监控周期信息
	 * 
	 * @author ShiGang @ 2013年10月15日
	 */
	public static class MonitorPeriodTimeInfo {

		public Date periodTime;

		public List<Date> dataTimes;
		// 本周期数据是否已经采全，默认true。当最后一个周期可以不完整，并且不完整时为false
		public Boolean isWhole = true;
		// 本周期在计算时是否能跳过，默认true。只有最后一个周期为false
		public Boolean canSkip = true;
	}

	/**
	 * <pre>
	 * 在DataRange 中获取最大的监控时间点，分钟为单位
	 * 
	 * @param offsets 时间偏侈列表
	 * @return
	 * </pre>
	 */
	public static int getMaxOffsetTimers(Map<String, List<Offset>> offsets) {
		int maxOffsetTime = 0;
		for (Entry<String, List<Offset>> entry : offsets.entrySet()) {
			List<Offset> offsetList = entry.getValue();
			for (Offset offset : offsetList) {
				if (offset.getFrom() > maxOffsetTime)
					maxOffsetTime = offset.getFrom();
			}
		}

		return maxOffsetTime;
	}

	/**
	 * <pre>
	 * 获取监控任务对应的时间偏移信息列表.以别名进行分组并排序
	 * 
	 * @param rangeList 时间范围列表
	 * @return List<RangeTimeOffset> 监控任务对应的时间偏移列表
	 * </pre>
	 */
	public static Map<String, List<Offset>> offsets(List<Range> rangeList) {
		Map<String, List<Offset>> offsets = new HashMap<String, List<Offset>>();
		if (rangeList == null || rangeList.isEmpty())
			return offsets;

		for (Range range : rangeList) {
			String alias = range.getAlias();
			List<Offset> offsetList = range.getOffsetList();
			// 把offset列表进行排序 方便后面处理时间信息
			Collections.sort(offsetList, new Comparator<Offset>() {

				public int compare(Offset current, Offset another) {
					if (current.getFrom() == another.getFrom() && current.getTo() == another.getTo())
						return 0;
					return current.getTo() < another.getTo() ? -1 : 1;
				}
			});
			offsets.put(alias, offsetList);
		}

		return offsets;
	}

	/**
	 * <pre>
	 * 过滤不符合分析周期规则的监控周期时间点
	 * 
	 * @param rule
	 * @param holidayPolicy
	 * @param dsGranularityUnit
	 * @param monitorTimeList
	 * @param enableHoliday
	 * @return
	 * </pre>
	 */
	private static List<MonitorPeriodTimeInfo> filterMonitorTimeListByAnalysisPeriodRule(Rule rule, Integer holidayPolicy, String dsGranularityUnit,
			List<Date> monitorTimeList, boolean enableHoliday) {
		List<MonitorPeriodTimeInfo> monitorPeriodInfoList = new LinkedList<MonitorPeriodTimeInfo>();
		String monitorPeriodUnit = rule.getPeriodInfo().getMonitorPeriod().getUnit().toString();

		// 将指定的监控周期
		String monitorPeriodAssignTime = rule.getPeriodInfo().getMonitorPeriod().getAssignMonitorTime();
		Set<String> set = null;
		if (StringUtils.isNotBlank(monitorPeriodAssignTime)) {
			set = split(monitorPeriodAssignTime);
		}

		String analysisPeriodUnit = rule.getPeriodInfo().getAnalysisPeriod().getUnit().toString();

		for (Date date : monitorTimeList) {
			// 是否是指定时间
			boolean bIsAssignTime = true;
			if (set != null) {
				// 当前监控周期的时间点，转换为指定单位后的值(要结合分析周期单位和指定时间单位)
				String indexValue = DateGranularityUtil.getValue(date, analysisPeriodUnit, monitorPeriodUnit);
				if (!set.contains(indexValue))
					bIsAssignTime = false;
			}

			// 如果不在指定时间范围内，则忽略当前时间点
			if (!bIsAssignTime)
				continue;

			/**
			 * 注：节假日判断只能是监控单位为天或小时; <br/>
			 * 如果监控周期的单位>天时，则无法用当前时间点判断节假日； <br/>
			 */
			if (monitorPeriodUnit.compareToIgnoreCase(DateGranularityUtil.DAY) == 0
					|| monitorPeriodUnit.compareToIgnoreCase(DateGranularityUtil.HOUR) == 0) {
				if (!HolidayChecker.matchHolidayPolicy(holidayPolicy, date, enableHoliday))
					continue;
			}

			// 按数据粒度单位，将监控时间点，转换成对应的数据时间点
			Date toDate = DateGranularityUtil.forwardTimeTravel(date, monitorPeriodUnit, 1);
			List<Date> dataTimeList = DateGranularityUtil.transformToSubDateList(date, toDate, dsGranularityUnit, true, false);

			// 过滤掉无效的数据时间点
			dataTimeList = filterDataTimeListByMonitorPeriodRule(rule, holidayPolicy, dataTimeList, enableHoliday);

			// 如果当前监控周期内，一个有效的数据时间点，都不存在，则忽略该监控周期
			if (dataTimeList == null || dataTimeList.size() < 1)
				continue;

			MonitorPeriodTimeInfo monitorPeriodInfo = new MonitorPeriodTimeInfo();
			monitorPeriodInfo.periodTime = date;
			monitorPeriodInfo.dataTimes = dataTimeList;

			monitorPeriodInfoList.add(monitorPeriodInfo);
		}

		return monitorPeriodInfoList;
	}

	/**
	 * <pre>
	 * 过滤符不合监控周期规则的数据时间点
	 * 
	 * @param rule
	 * @param holidayPolicy
	 * @param dataTimeList
	 * @param enableHoliday
	 * @return
	 * </pre>
	 */
	private static List<Date> filterDataTimeListByMonitorPeriodRule(Rule rule, Integer holidayPolicy, List<Date> dataTimeList, boolean enableHoliday) {
		List<Date> validDataTimeList = new LinkedList<Date>();
		String monitorPeriodUnit = rule.getPeriodInfo().getMonitorPeriod().getUnit().toString();

		AssignTime assignTime = rule.getPeriodInfo().getMonitorPeriod().getAssignTime();

		Set<String> set = null;
		String assignTimeUnit = null;
		if (assignTime != null) {
			assignTimeUnit = assignTime.getUnit().toString();
			set = split(assignTime.getValue());
		}

		for (Date date : dataTimeList) {
			// 是否是指定时间
			boolean bIsAssignTime = true;
			if (set != null) {
				// 当前监控周期的时间点，转换为指定单位后的值(要结合分析周期单位和指定时间单位)
				String indexValue = DateGranularityUtil.getValue(date, monitorPeriodUnit, assignTimeUnit);
				if (!set.contains(indexValue))
					bIsAssignTime = false;
			}

			// 如果不在指定时间范围内，则忽略当前时间点
			if (!bIsAssignTime)
				continue;

			// 如果当前时间点不在节假日之内，则忽略当前时间点
			if (!HolidayChecker.matchHolidayPolicy(holidayPolicy, date, enableHoliday))
				continue;

			validDataTimeList.add(date);
		}

		return validDataTimeList;
	}

	/**
	 * <pre>
	 * 依据规则获取监控时间点
	 * 
	 * @param analysisUnit 分析单位(不一定是监控任务的分析周期单位)
	 * @param maxAnalysisPeriodNum 分析单位数量(不一定是监控任务的分析周期单位数量)
	 * @param currRunTime 当前运行时间
	 * @param rule 监控规则
	 * @param holidayPolicy 数据节假日策略
	 * @param dsGranularityUnit 数据源单位
	 * @param enableHoliday 是否启用节假日
	 * @param skipInvalidToday 当最后一个监控周期无效时，是否跳出本方法并返回null
	 * @param delInvalidTime 是否删除无效的监控时间点
	 * @param 
	 * @return
	 * </pre>
	 */
	public static List<MonitorPeriodTimeInfo> getMonitorTimes(String analysisUnit, int maxAnalysisPeriodNum, Date currRunTime, Rule rule,
			Integer holidayPolicy, String dsGranularityUnit, boolean enableHoliday, boolean skipInvalidToday, boolean delInvalidTime) {
		// 所有监控单位的时间点(按降序排列)
		List<MonitorPeriodTimeInfo> allMonitorPeriodInfoList = new ArrayList<MonitorPeriodTimeInfo>();

		// 监控开始时间，按监控单位，取整
		long hitMilliSeconds = currRunTime.getTime();
		Date hitDate = new Date(hitMilliSeconds);
		/**
		 * 监控数据开始时间，以监控单位取整<br/>
		 * 例：curr_run_time == 2013-9-10 18:00:00, 数据粒度是小时 <br/>
		 * 假如监控周期(needWhole=1时):<br/>
		 * =1小时 则数据时间是:2013-9-10 18:00:00 <br/>
		 * =1天 则数据时间是:2013-9-9 00:00:00 至2013-9-9 23:00:00 <br/>
		 * =24小时 则数据时间是:2013-9-9 17:00:00 至2013-9-10 18:00:00 <br/>
		 * =1月 则数据时间是:2013-8-1 00:00:00 至2013-8-31 23:00:00 <br/>
		 * 
		 * 
		 * 假如监控周期(needWhole=0时):<br/>
		 * =1小时 则数据时间是:2013-9-10 18:00:00 <br/>
		 * =1天 则数据时间是:2013-9-10 00:00:00 至2013-9-10 18:00:00 <br/>
		 * =24小时 则数据时间是:2013-9-9 17:00:00 至2013-9-10 18:00:00 <br/>
		 * =1月 则数据时间是:2013-9-1 00:00:00 至2013-9-9 00:00:00 <br/>
		 */

		String monitorPeriodUnit = rule.getPeriodInfo().getMonitorPeriod().getUnit().toString();
		
		int needWhole = rule.getPeriodInfo().getMonitorPeriod().getNeedWhole();
		int occurNum = rule.getPeriodInfo().getOccurTimes().getSubOccurTimes().getValue();
		int nAnalysisPeriod = 0;
		if(needWhole==0){
			Date toDate = hitDate;
			Date fromDate = DateGranularityUtil.truncate(hitDate, monitorPeriodUnit);
			List<Date> monitorTimeList = DateGranularityUtil.transformToSubDateList(fromDate, toDate, monitorPeriodUnit, true, false);

			// 过滤掉不在当前规则内的的时间点，并找出每个监控时间点对应的数据时间点列表
			List<MonitorPeriodTimeInfo> currAnalysisMonitorTimeInfoList = filterMonitorTimeListByAnalysisPeriodRule(rule, holidayPolicy,
					dsGranularityUnit, monitorTimeList, enableHoliday);
			// 时间向前推移一个分析单位
			hitDate = fromDate;
			//XXX
			// 如果当前分析周期内，无合适的监控时间点和数据时间点，<br/>
			// 则当前分析周期不计在分析数量内，继续向前推一个分析周期时间单位，直至最少有一个时间点为止。 <br/>
			if ((null != currAnalysisMonitorTimeInfoList) && currAnalysisMonitorTimeInfoList.size() > 0){
				for (MonitorPeriodTimeInfo m : currAnalysisMonitorTimeInfoList) {
					// 最后一个周期不可跳过，因为没有保存
					m.canSkip = false;
					
					// 当前时间点之后的数据不存在，不参与运算
					List<Date> removeList = new ArrayList<Date>();
					for (Date date : m.dataTimes) {
						if(date.after(toDate)){
							removeList.add(date);
						}
					}
					// dataTimes中本来就是需要监控的时间点。
					// 如果有不可用点，说明今天数据不全。需要判断各种情况，防止无效运行
					// 如果无可用点，说明今天数据全了。
					if(!removeList.isEmpty()){
						// 只要有删除，本周期就肯定不全
						m.isWhole=false;
						if(delInvalidTime){
							m.dataTimes.removeAll(removeList);
							if(skipInvalidToday&&m.dataTimes.size()<occurNum){// 有效时间点个数不足最小频次，跳过
								return null;
							}
						}
					}
					// 如果当前时间点不在周期内，此次无需运行
					if(skipInvalidToday&&!m.dataTimes.contains(toDate)){
						return null;
					}
				}
				// 将当前分析周期内的有效监控时间点合并到总列表中
				allMonitorPeriodInfoList.addAll(currAnalysisMonitorTimeInfoList);
				++nAnalysisPeriod;
			}
			//XXX
		}else{
			hitDate = DateGranularityUtil.truncate(hitDate, monitorPeriodUnit);
		}
		// 如果监控时间单位和分析时间单位与数据时间粒度单位相等，则包含当前的数据时间点
		if (monitorPeriodUnit.equalsIgnoreCase(dsGranularityUnit) && analysisUnit.equalsIgnoreCase(dsGranularityUnit)) {
			// 往前推一个数据时间点, 因为(数据取时间点为>=fromDate < toDate)
			hitDate = DateGranularityUtil.forwardTimeTravel(hitDate, dsGranularityUnit, 1);
		}
		while (nAnalysisPeriod < maxAnalysisPeriodNum) {
			// (数据取时间点为>=fromDate < toDate)
			Date fromDate = DateGranularityUtil.backwardTimeTravel(hitDate, analysisUnit, 1);
			Date toDate = hitDate;

			/**
			 * 将当前的一个分析周期时间范围，以监控周期为单位，切分成N个监控周期时间点 <br/>
			 * 例： 分析周期是天，监控周期是小时， 如果CURR_RUN_TIME = 2013-9-10 <br/>
			 * 那么按监控周期单位切分后，则变为: <br/>
			 * 2013-9-9 00:00:00 <br/>
			 * 2013-9-9 01:00:00 <br/>
			 * .... <br/>
			 * 2013-9-9 23:00:00 <br/>
			 * 
			 */

			List<Date> monitorTimeList = DateGranularityUtil.transformToSubDateList(fromDate, toDate, monitorPeriodUnit, true, false);

			// 过滤掉不在当前规则内的的时间点，并找出每个监控时间点对应的数据时间点列表
			List<MonitorPeriodTimeInfo> currAnalysisMonitorTimeInfoList = filterMonitorTimeListByAnalysisPeriodRule(rule, holidayPolicy,
					dsGranularityUnit, monitorTimeList, enableHoliday);
			// 时间向前推移一个分析单位
			hitDate = fromDate;
			// 如果当前分析周期内，无合适的监控时间点和数据时间点，<br/>
			// 则当前分析周期不计在分析数量内，继续向前推一个分析周期时间单位，直至最少有一个时间点为止。 <br/>
			if (currAnalysisMonitorTimeInfoList == null || currAnalysisMonitorTimeInfoList.size() < 1)
				continue;

			// 将当前分析周期内的有效监控时间点合并到总列表中
			allMonitorPeriodInfoList.addAll(currAnalysisMonitorTimeInfoList);

			++nAnalysisPeriod;
		}

		// 将监控时间点，由高至低排序(高至低方便后面的data range类型取值)
		Collections.sort(allMonitorPeriodInfoList, new Comparator<MonitorPeriodTimeInfo>() {

			public int compare(MonitorPeriodTimeInfo current, MonitorPeriodTimeInfo another) {
				if (current.periodTime.getTime() == another.periodTime.getTime())
					return 0;
				return current.periodTime.getTime() > another.periodTime.getTime() ? -1 : 1;
			}
		});

		return allMonitorPeriodInfoList;
	}

	/**
	 * <pre>
	 * 求下一个任务的运行时
	 * 
	 * @param monitorTask
	 * @param enableHoliday
	 * @return
	 * </pre>
	 */
	public static Date nextMonitorTime(MonitorTask monitorTask, boolean enableHoliday) {
		Date nextMonitorTime = new Date(monitorTask.getCurrMonitorTime().getTime());
		int periodNum = monitorTask.getPeriodNum();
		TimeUnit gran = monitorTask.getPeriodUnit();
		Integer holidayPolicy = null;
		Filter filter = monitorTask.getFilter();
		if (filter != null) {
			Holiday holiday = filter.getHoliday();
			if (holiday != null) {
				holidayPolicy = holiday.getPolicy();
			}
		}

		String dsGranularityUnit = monitorTask.getDataSource().getGranularity().toString();
		Rule rule = monitorTask.getRule();

		int nPeriodIndex = 0;
		while (nPeriodIndex < periodNum) {
			Date previousDate = nextMonitorTime;
			nextMonitorTime = DateGranularityUtil.forwardTimeTravel(nextMonitorTime, gran.toString(), 1);

			// 监控时间点匹配
			Set<String> set = null;
			// 监控周期指定时间点和节假日配匹
			{
				boolean bMatched = false;
				if (StringUtils.isNotBlank(rule.getPeriodInfo().getMonitorPeriod().getAssignMonitorTime())) {
					set = split(rule.getPeriodInfo().getMonitorPeriod().getAssignMonitorTime());
				}

				String assignUnit = rule.getPeriodInfo().getMonitorPeriod().getUnit().toString();
				int needWhole = rule.getPeriodInfo().getMonitorPeriod().getNeedWhole();
				List<Date> subDateList = null;
				if(needWhole==0){
					assignUnit = rule.getPeriodInfo().getMonitorPeriod().getAssignTime().getUnit().toString();
					subDateList = DateGranularityUtil.transformToSubDateList(previousDate, nextMonitorTime, assignUnit, false, true);
				}else{
					subDateList = DateGranularityUtil.transformToSubDateList(previousDate, nextMonitorTime, assignUnit, false, true);
				}
				for (Date date : subDateList) {
					// 节假日判断(只能针对监控周期单位为日和小时的比较
					if (assignUnit.compareToIgnoreCase(DateGranularityUtil.DAY) == 0 ||
							assignUnit.compareToIgnoreCase(DateGranularityUtil.HOUR) == 0) {
						if (!HolidayChecker.matchHolidayPolicy(holidayPolicy, date, enableHoliday)) {
							continue;
						}
					}
					// 指定时间判断
					if (set != null && set.size() > 0) {
						// 当前监控周期的时间点，转换为指定单位后的值(要结合分析周期单位和指定时间单位)
						String indexValue = DateGranularityUtil.getValue(date, rule.getPeriodInfo().getAnalysisPeriod().getUnit().toString(),
								assignUnit);
						// 判断是否是在指定时间内
						if (set.contains(indexValue)) {
							bMatched = true;
							break;
						}
					} else {
						bMatched = true;
					}
				}

				// 如果没有一个时间是在指定时间内，则跳过该时间点
				if (!bMatched)
					continue;
			}

			// 清空set缓存
			if (set != null)
				set.clear();
			set = null;

			// 数据时间点和节假日配匹
			{
				boolean bMatched = false;
				String assignUnit = null;
				if (rule.getPeriodInfo().getMonitorPeriod().getAssignTime() != null) {
					set = split(rule.getPeriodInfo().getMonitorPeriod().getAssignTime().getValue());
					assignUnit = rule.getPeriodInfo().getMonitorPeriod().getAssignTime().getUnit().toString();
				}

				List<Date> subDateList = DateGranularityUtil.transformToSubDateList(previousDate, nextMonitorTime, dsGranularityUnit, false, true);
				for (Date date : subDateList) {
					// 节假日判断(只能针对数据周期单位为日和小时的比较
					if (dsGranularityUnit.compareToIgnoreCase(DateGranularityUtil.DAY) == 0
							|| dsGranularityUnit.compareToIgnoreCase(DateGranularityUtil.HOUR) == 0) {
						if (!HolidayChecker.matchHolidayPolicy(holidayPolicy, date, enableHoliday)) {
							continue;
						}
					}
					// 指定时间判断
					if (set != null && set.size() > 0) {
						// 当前监控周期的时间点，转换为指定单位后的值(要结合监控周期单位和指定时间单位)
						String indexValue = DateGranularityUtil.getValue(date, rule.getPeriodInfo().getMonitorPeriod().getUnit().toString(),
								assignUnit);
						// 判断是否是在指定时间内
						if (set.contains(indexValue)) {
							bMatched = true;
							break;
						}
					} else {
						bMatched = true;
					}
				}

				// 如果没有一个时间是在指定时间内，则跳过该时间点
				if (!bMatched)
					continue;
			}

			++nPeriodIndex;
		}

		return nextMonitorTime;
	}

	/**
	 * <pre>
	 * 拆分指定时间
	 * 指定时间信息以逗号分隔.
	 * 
	 * @param assignTimes
	 * @return
	 * </pre>
	 */
	public static Set<String> split(String assignTimes) {
		Set<String> set = new HashSet<String>();
		if (StringUtils.isBlank(assignTimes))
			return set;
		
		// 使用逗号拆分
		String[] array = assignTimes.split(",");
		for (String string : array)
			set.add(string.trim());
		return set;
	}

}
