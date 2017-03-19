package cn.uway.ubp.monitor.context;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.util.database.DatabaseUtil;

/**
 * 忙时管理器
 * TODO SQL to DAO
 * @author chenrongqiang @ 2013-11-11
 */
public class BusyHourManager {

	/**
	 * 定时调度线程池
	 */
	private static ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);

	/**
	 * 日志
	 */
	protected static final Logger logger = LoggerFactory.getLogger(BusyHourManager.class);

	/**
	 * C网忙时 缓存配置启用
	 */
	private static boolean ENABLE;

	/**
	 * 缓存 周期 （多久时间重新加载一次忙时数据） 单位小时
	 */
	private static long PERIOD;

	/**
	 * 缓存 时间（缓存多久的数据，以当前时间为基准往前推） 单位小时
	 */
	private static long CACHE_DAYS;

	/**
	 * level type Date nesysId 表 字段 忙时点 网元ID
	 */
	public static Map<Integer, Map<Integer, Map<Date, List<String>>>> BUSY_MAP = new HashMap<Integer, Map<Integer, Map<Date, List<String>>>>();

	public static Map<Integer, String> BUSY_MAP_TABLE = new HashMap<Integer, String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 4126576670080343577L;

		{
			put(1, "ds_perf_busy_carrier_1x_d");
			put(2, "ds_perf_busy_carrier_do_d");
			put(3, "ds_perf_busy_cell_d");
			put(4, "ds_perf_busy_bts_d");
			put(5, "ds_perf_busy_bsc_d");
			put(6, "ds_perf_busy_city_d");
			put(7, "ds_perf_busy_province_d");
		}
	};

	public static Map<Integer, String> BUSY_FIELD = new HashMap<Integer, String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 3951894520928711198L;

		{
			put(1, "BUSY_TIME_MORNING_1X");
			put(2, "BUSY_TIME_EVENING_1X");
			put(3, "BUSY_TIME_DAY_1X");
			put(4, "BUSY_TIME_MORNING_DO");
			put(5, "BUSY_TIME_EVENING_DO");
			put(6, "BUSY_TIME_DAY_DO");
		}
	};

	/**
	 * 忙时管理器初始化方法
	 */
	public static void initialize() {
		ENABLE = Configuration.getBoolean(Configuration.BUSY_HOUR_ENABLE);
		if (!ENABLE) {
			logger.warn("忙时开关已关闭，忙时监控无效，请确认");
			return;
		}
		
		PERIOD = Configuration.getInteger(Configuration.BUSY_HOUR_REFRESH_PERIOD);
		CACHE_DAYS = Configuration.getInteger(Configuration.BUSY_HOUR_CACHE_DAYS);
		start();
	}

	/**
	 * 任务一天加载一次，加载30天忙时数据
	 */
	private static void start() {
		// 没有启用返回
		if (!ENABLE)
			return;
		
		final Runnable busyhandler = new Runnable() {

			public void run() {
				Thread.currentThread().setName("忙时数据加载");
				
				Connection conn = null;
				PreparedStatement ps = null;
				ResultSet rs = null;
				try {
					conn = DbPoolManager.getConnectionForBusyHour();

					for (Entry<Integer, String> entrytable : BUSY_MAP_TABLE.entrySet()) {
						Map<Integer, Map<Date, List<String>>> tableMap = BUSY_MAP.get(entrytable.getKey());
						if (tableMap == null) {
							tableMap = new HashMap<Integer, Map<Date, List<String>>>();
							BUSY_MAP.put(entrytable.getKey(), tableMap);
						}
						StringBuilder sql = new StringBuilder();
						// 取30天的数据缓存
						sql.append("select * from ").append(entrytable.getValue())
								.append(" where SYSDATE>=start_time AND SYSDATE-" + CACHE_DAYS + "<=start_time");
						String sqlStr = sql.toString();
						logger.debug("载入忙时数据，SQL：{}", sqlStr);
						ps = conn.prepareStatement(sqlStr);
						rs = ps.executeQuery();
						while (rs.next()) {
							for (Entry<Integer, String> entryField : BUSY_FIELD.entrySet()) {
								try {
									Map<Date, List<String>> fieldMap = tableMap.get(entryField.getKey());
									if (fieldMap == null) {
										fieldMap = new HashMap<Date, List<String>>();
										tableMap.put(entryField.getKey(), fieldMap);
									}
									Date busyTime = rs.getDate(entryField.getValue());
									if (busyTime == null)
										continue;
									List<String> neList = fieldMap.get(busyTime);
									if (neList == null) {
										neList = new ArrayList<String>();
										fieldMap.put(busyTime, neList);
									}
									// 网元ID
									if (entrytable.getKey() < 6)
										neList.add(rs.getString("NE_SYS_ID"));
									// 城市
									if (entrytable.getKey() == 6)
										neList.add(rs.getString("CITY_ID"));
									if (entrytable.getKey() == 7)
										// 0为全省
										neList.add("0");
								} catch (SQLException e) {
									continue;
								}
							}
						}
					}

				} catch (SQLException e) {
					logger.error("载入忙时数据失败", e);
				} finally {
					DatabaseUtil.close(conn, ps, rs);
				}
			}
		};
		SCHEDULER.scheduleAtFixedRate(busyhandler, 0, PERIOD, TimeUnit.MINUTES);
	}
	
}
