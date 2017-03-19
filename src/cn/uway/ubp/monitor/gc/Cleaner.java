package cn.uway.ubp.monitor.gc;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;

import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.util.DateTimeUtil;
import cn.uway.framework.util.FileUtil;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.ubp.monitor.context.DatasourceAccessLog;
import cn.uway.ubp.monitor.context.DbPoolManager;
import cn.uway.ubp.monitor.context.EventAccessLog;
import cn.uway.ubp.monitor.dao.DataSourceDAO;
import cn.uway.ubp.monitor.dao.MonitorTaskDAO;
import cn.uway.ubp.monitor.data.BlockDataProviderImpl;
import cn.uway.ubp.monitor.event.LocalEventAccessor;
import cn.uway.util.DateGranularityUtil;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.MonitorTask;

/**
 * <pre>
 * 定时清理作业，由CollectionDepot定时调度
 * 完成UBP中数据源、事件的回收工作
 * 
 * @author chenrongqiang 2013-6-1
 * </pre>
 */
public class Cleaner extends TimerTask {

	/**
	 * 在最小访问数据时间基础上的滑动时间
	 */
	private final int slideHours;

	/**
	 * 数据文件存储时间 单位：小时
	 */
	private final long liveHours;

	private static final Logger logger = LoggerFactory.getLogger(Cleaner.class);

	public Cleaner(int slideHours, long liveHours) {
		this.slideHours = slideHours;
		this.liveHours = liveHours;
	}

	@Override
	public void run() {
		Connection taskConn = null;
		try {
			taskConn = DbPoolManager.getConnectionForTask();
			List<DataSource> dataSourceList = DataSourceDAO.getInstance().getDataSources(taskConn);
			Map<Long, Date> rawDataAccessLogs = DatasourceAccessLog.getAccessLog();
			Map<Long, Date> eventAccessLogs = EventAccessLog.getAccessLog();
			// 以数据源为单位进行处理
			for (DataSource dataSource : dataSourceList) {
				cleanRawData(taskConn, dataSource, rawDataAccessLogs);
				cleanEvent(taskConn, dataSource, eventAccessLogs);
			}
		} catch (Exception e) {
			logger.error("清理失败", e);
		}finally {
			DatabaseUtil.close(taskConn);
		}
	}

	/**
	 * <pre>
	 * 原始数据清理 读取DatasourceAccessLog中每个数据源的使用情况
	 * 读取DatasourceAccessLog中的数据源访问的最小时间对应的事件+偏移时间
	 * @throws SQLException
	 * @throws DocumentException
	 * </pre>
	 */
	private void cleanRawData(Connection conn, DataSource dataSource, Map<Long, Date> accessLogs) throws SQLException, DocumentException {
		String cacheFileDirectory = findDirectory(dataSource);
		File rootFile = new File(cacheFileDirectory);
		// 目录不存在 直接退出
		if (!rootFile.exists())
			return;
		Date leastAccessTime = accessLogs.get(dataSource.getId());
		if (leastAccessTime == null)
			leastAccessTime = cleanNoUserRawDataWithDate(conn, dataSource.getId());
		final String retainFileName = this.getRetainFileName(leastAccessTime);
		File[] deleteFiles = rootFile.listFiles(new FileFilter() {

			public boolean accept(File pathFile) {
				return ((pathFile.getName().indexOf(".index.") == -1) && (pathFile.getName().indexOf(".event.") == -1)
						&& (pathFile.getName().substring(0, 12).compareTo(retainFileName) < 0) && (System.currentTimeMillis()
						- pathFile.lastModified() > liveHours));
			}
		});
		for (File file : deleteFiles) {
			file.delete();
		}
		// 删除成功后需要将leastAccessTime清理
		DatasourceAccessLog.remove(dataSource.getId());
	}

	/**
	 * <pre>
	 * 如果数据源没有访问记录.则需要判断当前数据源对应所有任务的最大分析周期+偏移时间
	 * 
	 * @param dataSourceId
	 * @return
	 * @throws SQLException 
	 * @throws DocumentException
	 * </pre>
	 */
	private Date cleanNoUserRawDataWithDate(Connection conn, long dataSourceId) throws SQLException, DocumentException {
		List<MonitorTask> monitorTaskList = MonitorTaskDAO.getInstance().getTasks(conn, dataSourceId);
		String gran = getMaxAnalysisUint(monitorTaskList);
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.HOUR, -slideHours);

		return DateGranularityUtil.backwardTimeTravel(
				DateTimeUtil.parseDateTimeNoEx(DateTimeUtil.format(calendar.getTime(), "yyyy-MM-dd HH") + ":00:00"), gran, 1);
	}

	/**
	 * <pre>
	 * 如果事件没有访问记录.则以当前监控任务分析周期+偏移时间
	 * 
	 * @param taskId
	 * @return
	 * </pre>
	 */
	private Date cleanNoUserRawEventWithDate(MonitorTask monitorTask) {
		String gran = monitorTask.getRule().getPeriodInfo().getAnalysisPeriod().getUnit().toString();
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.HOUR, -slideHours);

		return DateGranularityUtil.backwardTimeTravel(
				DateTimeUtil.parseDateTimeNoEx(DateTimeUtil.format(calendar.getTime(), "yyyy-MM-dd HH") + ":00:00"), gran, 1);
	}

	private String getMaxAnalysisUint(List<MonitorTask> monitorTaskList) {
		String gran = DateGranularityUtil.HOUR;
		for (MonitorTask monitorTask : monitorTaskList) {
			String analysisPeriodUnit = monitorTask.getRule().getPeriodInfo().getAnalysisPeriod().getUnit().toString();

			if (DateGranularityUtil.compare(gran, analysisPeriodUnit) < 0) {
				gran = analysisPeriodUnit;
			}
		}
		return gran;
	}

	/**
	 * <pre>
	 * 清理事件缓存文件
	 * 事件的清理有两种机制
	 * 1、任务被删除时.直接删除事件缓存文件
	 * 2、任务未被删除.则读取EventAccessLog中的任务访问的最小时间对应的事件+偏移时间
	 * @throws SQLException 
	 * @throws DocumentException
	 * </pre>
	 */
	void cleanEvent(Connection conn, DataSource dataSource, Map<Long, Date> accessLogs) throws SQLException, DocumentException {
		if (dataSource == null)
			return;
		String rootFile = findDirectory(dataSource);
		if (rootFile == null || !FileUtil.exists(rootFile))
			return;
		List<MonitorTask> monitorTasks = MonitorTaskDAO.getInstance().getTasks(conn, dataSource.getId());
		Iterator<MonitorTask> iterator = monitorTasks.iterator();
		while (iterator.hasNext()) {
			MonitorTask monitorTask = iterator.next();
			LocalEventAccessor localEventAccessor = LocalEventAccessor.getInstance();
			try {
				localEventAccessor.lock();
				localEventAccessor.apply(rootFile, monitorTask.getTaskId());
				if (monitorTask.isDeleted())
					localEventAccessor.delete();
				else {
					Date leastAccessTime = accessLogs.get(monitorTask.getTaskId());
					if (leastAccessTime == null)
						leastAccessTime = cleanNoUserRawEventWithDate(monitorTask);
					relocate(localEventAccessor, leastAccessTime);
				}
			} catch (Exception e) {

			} finally {
				localEventAccessor.unLock();
			}
			// 删除成功后需要将leastAccessTime清理
			EventAccessLog.remove(monitorTask.getTaskId());
		}
	}

	private void relocate(LocalEventAccessor localEventAccessor, Date leastAccessTime) throws IOException {
		if (leastAccessTime == null)
			return;
		Date retainTime = getRetainTime(leastAccessTime);
		localEventAccessor.relocate(retainTime);
	}

	private String getRetainFileName(Date leastAccessDate) {
		return DateTimeUtil.formatDateTimeStr(leastAccessDate);
	}

	private Date getRetainTime(Date leastAccessDate) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(leastAccessDate);
		calendar.add(Calendar.HOUR, -slideHours);
		return calendar.getTime();
	}

	private String findDirectory(DataSource dataSource) {
		if (dataSource == null)
			return null;

		return BlockDataProviderImpl.getInstance().getPath(dataSource);
	}

}
