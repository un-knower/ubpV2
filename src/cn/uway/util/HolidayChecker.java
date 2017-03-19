package cn.uway.util;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * <pre>
 * 节假日处理
 * 包含两部分功能:
 * 1,初始化节假日数据
 * 2,定义调度,定时刷新节假日数据
 * </pre>
 * 
 * @author Chris 2014年5月5日
 */
public class HolidayChecker {

	// 节假日缓存容器
	private static Set<Long> holidaySet;

	/**
	 * 读写锁应用于"安全信息缓存表"被刷新替换时 非严谨的做法,其实不加锁也没有太大问题
	 */
	private static ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	private static ReadLock rl = rwl.readLock();
	private static WriteLock wl = rwl.writeLock();

	/**
	 * <pre>
	 * 刷新节假日缓存(直接替换)
	 * 
	 * @param set 待替换的数据
	 * </pre>
	 */
	public static void refreshData(Set<Long> set) {
		wl.lock();
		try {
			holidaySet = set;
		} finally {
			wl.unlock();
		}
	}

	/**
	 * <pre>
	 * 节假日算法校验
	 * 
	 * @param policy
	 * @param date
	 * @param enableHoliday
	 * @return 是否满足节假日算法
	 * </pre>
	 */
	public static boolean matchHolidayPolicy(Integer policy, Date date, boolean enableHoliday) {
		// 未启用节假日视为所有时间均可运行
		if (policy == null || policy == Constants.ALL_DAY || !enableHoliday)
			return true;
		if (policy == Constants.WORKDAY_ONLY && !isHoliday(date))
			return true;
		if (policy == Constants.HOLIDAY_ONLY && isHoliday(date))
			return true;
		return false;
	}

	/**
	 * <pre>
	 * 验证日期是否节假日
	 * 
	 * @param date 日期
	 * @return 是节假日返回true,否则返回false
	 * </pre>
	 */
	public static boolean isHoliday(Date date) {
		if (date == null)
			throw new IllegalArgumentException("等验证的日期null");
		
		return isHoliday(DateGranularityUtil.truncate(date, DateGranularityUtil.DAY).getTime());
	}

	/**
	 * <pre>
	 * 验证日期是否节假日
	 * 
	 * @param timestamp 时间截
	 * @return 是节假日返回true,否则返回false
	 * </pre>
	 */
	public static boolean isHoliday(long timestamp) {
		rl.lock();
		try {
			/*
			// 不启用节假日检查
			if (!ENABLE)
				return false;
			*/
			// 取日期部分 减除时区时间
			// timestamp -= timestamp % (24 * 60 * 60 * 1000) +
			// TimeZone.getDefault().getRawOffset();
			return holidaySet.contains(timestamp);
		} finally {
			rl.unlock();
		}
	}

}
