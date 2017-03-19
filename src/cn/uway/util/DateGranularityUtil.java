package cn.uway.util;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <pre>
 * UBP监控模块时间粒度工具类
 * @author chenrongqiang 2013-6-7
 * </pre>
 */
public final class DateGranularityUtil {
	
	/**
	 * 1分钟粒度
	 */
	public static final String _1MINUTE = "_1MINUTE";
	
	/**
	 * 5分钟粒度
	 */
	public static final String _5MINUTE = "_5MINUTE";
	
	/**
	 * 10分钟粒度
	 */
	public static final String _10MINUTE = "_10MINUTE";
	
	/**
	 * 15分钟粒度
	 */
	public static final String _15MINUTE = "_15MINUTE";
	
	/**
	 * 30分钟粒度
	 */
	public static final String _30MINUTE = "_30MINUTE";

	/**
	 * 小时粒度
	 */
	public static final String HOUR = "HOUR";

	/**
	 * 天粒度
	 */
	public static final String DAY = "DAY";

	/**
	 * 周粒度
	 */
	public static final String WEEK = "WEEK";

	/**
	 * 月粒度
	 */
	public static final String MONTH = "MONTH";

	/**
	 * 季度粒度
	 */
	public static final String SEASON = "SEASON";

	/**
	 * 年粒度
	 */
	public static final String YEAR = "YEAR";

	/**
	 * 优先级MAP
	 */
	public static Map<String, Integer> priorityByStrMap = new HashMap<String, Integer>(8);

	/**
	 * 优先级MAP
	 */
	public static Map<Integer, String> priorityByNumMap = new HashMap<Integer, String>(8);

	static {
		priorityByStrMap.put(_1MINUTE, 1);
		priorityByStrMap.put(_5MINUTE, 2);
		priorityByStrMap.put(_10MINUTE, 3);
		priorityByStrMap.put(_15MINUTE, 4);
		priorityByStrMap.put(_30MINUTE, 5);
		priorityByStrMap.put(HOUR, 6);
		priorityByStrMap.put(DAY, 7);
		priorityByStrMap.put(WEEK, 8);
		priorityByStrMap.put(MONTH, 9);
		priorityByStrMap.put(SEASON, 10);
		priorityByStrMap.put(YEAR, 11);
		priorityByNumMap.put(1, _1MINUTE);
		priorityByNumMap.put(2, _5MINUTE);
		priorityByNumMap.put(3, _10MINUTE);
		priorityByNumMap.put(4, _15MINUTE);
		priorityByNumMap.put(5, _30MINUTE);
		priorityByNumMap.put(6, HOUR);
		priorityByNumMap.put(7, DAY);
		priorityByNumMap.put(8, WEEK);
		priorityByNumMap.put(9, MONTH);
		priorityByNumMap.put(10, SEASON);
		priorityByNumMap.put(11, YEAR);
	}

	/**
	 * 判断是否是UBP中合法的粒度定义
	 * 
	 * @param gran
	 * @return 否是UBP中合法的粒度定义
	 */
	public static boolean valideGranularity(String gran) {
		if (gran == null || "".equals(gran))
			return false;
		
		return _1MINUTE.equalsIgnoreCase(gran) || _5MINUTE.equalsIgnoreCase(gran) || _10MINUTE.equalsIgnoreCase(gran)
				|| _15MINUTE.equalsIgnoreCase(gran) || _30MINUTE.equalsIgnoreCase(gran) || HOUR.equalsIgnoreCase(gran)
				|| DAY.equalsIgnoreCase(gran) || WEEK.equalsIgnoreCase(gran) || MONTH.equalsIgnoreCase(gran)
				|| SEASON.equalsIgnoreCase(gran) || YEAR.equalsIgnoreCase(gran);
	}

	/**
	 * 时间滑动,在给定时间的基础上往历史时间滑动一定粒度单位的时间,granNum 数据取值为正整数据和零>
	 * 
	 * @return
	 */
	public static Date backwardTimeTravel(Date date, String gran, int granNum) {
		if (!valideGranularity(gran))
			throw new IllegalArgumentException("非法的时间粒度" + gran);
		
		if (granNum <= 0)
			return date;
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		switch (gran) {
			case _1MINUTE :
				calendar.add(Calendar.MINUTE, -granNum);
				break;
			case _5MINUTE :
				calendar.add(Calendar.MINUTE, -granNum*5);
				break;
			case _10MINUTE :
				calendar.add(Calendar.MINUTE, -granNum*10);
				break;
			case _15MINUTE :
				calendar.add(Calendar.MINUTE, -granNum*15);
				break;
			case _30MINUTE :
				calendar.add(Calendar.MINUTE, -granNum*30);
				break;
			case HOUR :
				calendar.add(Calendar.HOUR_OF_DAY, -granNum);
				break;
			case DAY :
				calendar.add(Calendar.DAY_OF_MONTH, -granNum);
				break;
			case WEEK :
				calendar.add(Calendar.WEEK_OF_YEAR, -granNum);
				break;
			case MONTH :
				calendar.add(Calendar.MONTH, -granNum);
				break;
			case SEASON :
				calendar.add(Calendar.MONTH, -granNum * 3);
				break;
			case YEAR :
				calendar.add(Calendar.YEAR, -granNum);
				break;
		}
		
		return calendar.getTime();
	}

	/**
	 * 时间滑动,在给定时间的基础上往未来时间滑动一定粒度单位的时间 granNum数据取值为正整数据和零<br>
	 * 
	 * @return
	 */
	public static Date forwardTimeTravel(Date date, String gran, int granNum) {
		if (!valideGranularity(gran))
			throw new IllegalArgumentException("非法的时间粒度" + gran);
		
		if (granNum <= 0)
			return date;
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		switch (gran) {
			case _1MINUTE :
				calendar.add(Calendar.MINUTE, granNum);
				break;
			case _5MINUTE :
				calendar.add(Calendar.MINUTE, granNum*5);
				break;
			case _10MINUTE :
				calendar.add(Calendar.MINUTE, granNum*10);
				break;
			case _15MINUTE :
				calendar.add(Calendar.MINUTE, granNum*15);
				break;
			case _30MINUTE :
				calendar.add(Calendar.MINUTE, granNum*30);
				break;
			case HOUR :
				calendar.add(Calendar.HOUR_OF_DAY, granNum);
				break;
			case DAY :
				calendar.add(Calendar.DAY_OF_MONTH, granNum);
				break;
			case WEEK :
				calendar.add(Calendar.WEEK_OF_YEAR, granNum);
				break;
			case MONTH :
				calendar.add(Calendar.MONTH, granNum);
				break;
			case SEASON :
				calendar.add(Calendar.MONTH, granNum * 3);
				break;
			case YEAR :
				calendar.add(Calendar.YEAR, granNum);
				break;
		}
		
		return calendar.getTime();
	}

	/**
	 * 时间粒度比较
	 * 
	 * @param gran1
	 * @param gran2
	 * @return
	 */
	public static int compare(String gran1, String gran2) {
		if (!valideGranularity(gran1) || !valideGranularity(gran2))
			throw new IllegalArgumentException("非法的时间粒度");
		
		if (gran1.equalsIgnoreCase(gran2))
			return 0;
		
		int priority1 = priorityByStrMap.get(gran1);
		int priority2 = priorityByStrMap.get(gran2);
		return priority1 > priority2 ? 1 : -1;
	}

	/**
	 * 获取给出时间的指定粒度的值<br>
	 * 
	 * @param dateTime
	 * @param gran
	 * @return granValue
	 */
	private static String granValue(Date dateTime, String gran) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(dateTime);
		String granValue = null;
		
		switch (gran) {
			case _1MINUTE :
				granValue = String.valueOf(getMinute(calendar.get(Calendar.MINUTE), _1MINUTE));
				break;
			case _5MINUTE :
				granValue = String.valueOf(getMinute(calendar.get(Calendar.MINUTE), _5MINUTE));
				break;
			case _10MINUTE :
				granValue = String.valueOf(getMinute(calendar.get(Calendar.MINUTE), _10MINUTE));
				break;
			case _15MINUTE :
				granValue = String.valueOf(getMinute(calendar.get(Calendar.MINUTE), _15MINUTE));
				break;
			case _30MINUTE :
				granValue = String.valueOf(getMinute(calendar.get(Calendar.MINUTE), _30MINUTE));
				break;
			case HOUR :
				granValue = String.valueOf(calendar.get(Calendar.HOUR_OF_DAY));
				break;
			case DAY :
				granValue = String.valueOf(calendar.get(Calendar.DATE));
				break;
			case WEEK :
				granValue = String.valueOf(calendar.get(Calendar.WEEK_OF_YEAR));
				break;
			case MONTH :
				granValue = String.valueOf(calendar.get(Calendar.MONTH) + 1);
				break;
			case SEASON :
				granValue = String.valueOf(season(calendar));
				break;
			case YEAR :
				granValue = String.valueOf(calendar.get(Calendar.YEAR));
				break;
		}
		
		return granValue;
	}

	/**
	 * 季度
	 * 
	 * @param calendar
	 * @return
	 */
	public static int season(Calendar calendar) {
		int month = calendar.get(Calendar.MONTH) + 1;
		if (month <= 3)
			return 1;
		if (month <= 6)
			return 2;
		if (month <= 9)
			return 3;
		return 4;
	}
	
	private static int getMinute(int minute, String gran) {
		switch (gran) {
			case _1MINUTE :
				return minute+1;
			case _5MINUTE :
				return (minute/5)+1;
			case _10MINUTE :
				return (minute/10)+1;
			case _15MINUTE :
				return (minute/15)+1;
			case _30MINUTE :
				return (minute/30)+1;
			default :
				throw new IllegalArgumentException("参数非法,无效的分钟粒度" + gran);
		}
	}

	/**
	 * 季度
	 * 
	 * @param calendar
	 * @return
	 */
	public static int season(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return season(calendar);
	}

	public static String getValue(Date date, String unit, String subUnit) {
		switch (subUnit) {
			case _1MINUTE :
				return granValue(date, subUnit);
			case _5MINUTE :
				return granValue(date, subUnit);
			case _10MINUTE :
				return granValue(date, subUnit);
			case _15MINUTE :
				return granValue(date, subUnit);
			case _30MINUTE :
				return granValue(date, subUnit);
			case HOUR :
				return granValue(date, subUnit);
			case DAY :
				switch (unit) {
					case DAY :
						return getValue(date, Calendar.DAY_OF_MONTH);
					case MONTH :
						return getValue(date, Calendar.DAY_OF_MONTH);
					case WEEK :
						return getValue(date, Calendar.DAY_OF_WEEK);
					case YEAR :
						return getValue(date, Calendar.DAY_OF_YEAR);
				}
			case WEEK :
				switch (unit) {
					case WEEK :
						return getValue(date, Calendar.WEEK_OF_YEAR);
					case MONTH :
						return getValue(date, Calendar.WEEK_OF_MONTH);
					case YEAR :
						return getValue(date, Calendar.WEEK_OF_YEAR);
				}
			case MONTH :
				switch (unit) {
					case MONTH :
						return getValue(date, Calendar.MONTH);
					case SEASON :
						return String.valueOf(DateGranularityUtil.season(date));
					case YEAR :
						return getValue(date, Calendar.MONTH);
				}
			case SEASON :
				if (DateGranularityUtil.SEASON.equals(unit) || DateGranularityUtil.YEAR.equals(unit))
					return String.valueOf(DateGranularityUtil.season(date));
			case YEAR :
				if (DateGranularityUtil.YEAR.equals(unit))
					return getValue(date, Calendar.YEAR);
		}
		
		throw new IllegalArgumentException("参数非法,上级时间单位" + unit + ",下级时间单位" + subUnit);
	}

	/**
	 * 获取指定时间的属性值
	 * 
	 * @param date
	 *            指定时间
	 * @param field
	 *            Calendar.FIELD
	 * @return 指定时间的属性值
	 */
	public static String getValue(Date date, int field) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		
		if (field == Calendar.MONTH)
			return String.valueOf(calendar.get(field) + 1);
		
		return String.valueOf(calendar.get(field));
	}

	public static int minutes(String gran) {
		if (!valideGranularity(gran))
			throw new IllegalArgumentException("参数错误.错误的数据源粒度定义：" + gran);
		
		switch (gran) {
			case _1MINUTE :
				return 1;
			case _5MINUTE :
				return 5;
			case _10MINUTE :
				return 10;
			case _15MINUTE :
				return 15;
			case _30MINUTE :
				return 30;
			case HOUR :
				return 60;
			case DAY :
				return 1440;
			case WEEK :
				return 10080;
			case MONTH :
				return 44640; // 一个月已最大31天来计算 用于精确计算会有问题
			case SEASON :
				return 133920;
			case YEAR :
				return 525600;
			default :
				throw new IllegalArgumentException("参数错误.错误的数据源粒度定义：" + gran);
		}
	}

	public static Date truncate(Date date, String gran) {
		if (!valideGranularity(gran))
			throw new IllegalArgumentException("参数错误.错误的数据源粒度定义：" + gran);

		Calendar calender = Calendar.getInstance();
		calender.setTime(date);
		
		switch (gran) {
			case _1MINUTE :
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			case _5MINUTE :
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			case _10MINUTE :
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			case _15MINUTE :
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			case _30MINUTE :
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			case HOUR :
				calender.set(Calendar.MINUTE, 0);
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			case DAY :
				calender.set(Calendar.HOUR_OF_DAY, 0);
				calender.set(Calendar.MINUTE, 0);
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			case WEEK :
				calender.set(Calendar.DAY_OF_WEEK, 1);
				calender.set(Calendar.HOUR_OF_DAY, 0);
				calender.set(Calendar.MINUTE, 0);
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			case MONTH :
				calender.set(Calendar.DAY_OF_MONTH, 1);
				calender.set(Calendar.HOUR_OF_DAY, 0);
				calender.set(Calendar.MINUTE, 0);
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			case SEASON :
				calender.set(Calendar.MONTH, calender.get(Calendar.MONTH) - (calender.get(Calendar.MONTH) % 3));
				calender.set(Calendar.DAY_OF_MONTH, 1);
				calender.set(Calendar.HOUR_OF_DAY, 0);
				calender.set(Calendar.MINUTE, 0);
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			case YEAR :
				calender.set(Calendar.MONTH, 0);
				calender.set(Calendar.DAY_OF_MONTH, 1);
				calender.set(Calendar.HOUR_OF_DAY, 0);
				calender.set(Calendar.MINUTE, 0);
				calender.set(Calendar.SECOND, 0);
				calender.set(Calendar.MILLISECOND, 0);
				return calender.getTime();
			default :
				throw new IllegalArgumentException("参数错误.错误的数据源粒度定义：" + gran);
		}
	}

	// 将一个时间段的时间，从fromDate开始，分割成以granularity为粒度的数据时间点
	public static List<Date> transformToSubDateList(Date fromDate, Date toDate, String granularity, boolean bIncludeFromDate, boolean bIncludeToDate) {
		List<Date> dateList = new ArrayList<Date>();

		Date hitDate = new Date(fromDate.getTime());

		if (bIncludeFromDate)
			dateList.add(hitDate);

		while (true) {
			hitDate = DateGranularityUtil.forwardTimeTravel(hitDate, granularity, 1);
			if (hitDate.compareTo(toDate) < 0)
				dateList.add(hitDate);
			else
				break;
		}

		if (bIncludeToDate && hitDate.compareTo(toDate) == 0)
			dateList.add(hitDate);

		return dateList;
	}

}
