package cn.uway.ubp.monitor.task;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.ubp.monitor.context.DbPoolManager;
import cn.uway.ubp.monitor.dao.DataSourceDAO;
import cn.uway.ubp.monitor.dao.MonitorTaskDAO;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.MonitorTask;

/**
 * 监控任务加载类
 * 
 * @author chenrongqiang @ 2013-8-5
 */
public class MonitorTaskLoader extends TimerTask {

	/**
	 * 以monitorTaskId进行分组的任务队列
	 */
	private JobGroupQueue<JobGroup> jobGroupQueue;

	/**
	 * 正在运行的任务列表
	 */
	private ConcurrentHashMap<String, JobGroup> runingJobMap;

	/**
	 * 日志
	 */
	private static final Logger logger = LoggerFactory
			.getLogger(TimerTaskLoader.class);

	/**
	 * <pre>
	 * 构造方法
	 * @param taskGroupQueue 以monitorTaskId进行分组的任务队列
	 * </pre>
	 */
	public MonitorTaskLoader(JobGroupQueue<JobGroup> taskGroupQueue,
			ConcurrentHashMap<String, JobGroup> runingJobMap) {
		this.jobGroupQueue = taskGroupQueue;
		this.runingJobMap = runingJobMap;
	}

	@Override
	public void run() {
		Thread.currentThread().setName("监控任务加载线程");
		Connection conn = null;
		try {
			conn = DbPoolManager.getConnectionForTask();
			// 取所有数据源信息,备用
			Map<Long, DataSource> allDataSource = DataSourceDAO.getInstance()
					.getAllDataSource(conn);

			// 获取所有满足运算的计算任务
			List<MonitorTask> expressionTasks = MonitorTaskDAO.getInstance()
					.getExpressionTasksByTaskDistribute(conn);
			if (!expressionTasks.isEmpty()) {
				processMonitorTask(expressionTasks, allDataSource);
			} else {
				logger.debug("本次未能找到需要执行指标运算的任务");
			}
		} catch (InterruptedException e) {
			logger.error("添加任务组至任务组队列时被中断", e);
		} catch (SQLException e) {
			logger.error("查找监控任务失败", e);
		}finally {
			DatabaseUtil.close(conn);
		}
	}

	public static String MakeReCalcRuningTaskKey(Long dsID, Long taskMonitorTime) {
		return dsID + "_" + taskMonitorTime;
	}

	public void processMonitorTask(List<MonitorTask> expressionTasks,
			Map<Long, DataSource> allDataSource) throws InterruptedException {
		// 将子任务转换为任务组
		Collection<JobGroup> jobGroupList = toGroup(expressionTasks,
				allDataSource);
		logger.debug("本次找到{}个需要执行的任务，分{}组", expressionTasks.size(),
				jobGroupList.size());

		processMonitorTask(jobGroupList);
	}

	public void processMonitorTask(Collection<JobGroup> jobGroupList)
			throws InterruptedException {
		for (JobGroup taskJob : jobGroupList) {
			String monitorTaskId = taskJob.getMonitorTaskId();
			if (runingJobMap.containsKey(monitorTaskId)) {
				logger.debug("监控任务{}已在任务列表或正在运行中", monitorTaskId);

				continue;
			}

			// runingTaskMap 任务在加载时添加进去，在运行完后移除，避免同一个时间段的任务被加载多次;
			runingJobMap.put(monitorTaskId, taskJob);
			jobGroupQueue.put(taskJob);
		}
	}

	/**
	 * <pre>
	 * 将查询出来的子任务分组(原始任务形式)
	 * 分组依据:相同的monitor_task_id
	 * @param taskList 任务列表
	 * @param allDataSource 所有数据源信息
	 * @return 以monitorTaskId进行分组的任务分组
	 * </pre>
	 */
	private Collection<JobGroup> toGroup(List<MonitorTask> taskList,
			Map<Long, DataSource> allDataSource) {
		// 以monitorTaskId进行分组 key:monitor_task_id value:MonitorTaskGroup
		Map<String, JobGroup> jobGroupMap = new HashMap<String, JobGroup>();
		// 循环任务列别进行分组
		for (MonitorTask task : taskList) {
			DataSource dataSource = allDataSource.get(task.getDataSourceId());
			if (dataSource == null) {
				logger.warn("任务{}找不到对应的数据源{}", task.getTaskId(),
						task.getDataSourceId());
				continue;
			}

			String monitorTaskId = task.getMonitorTaskId();
			JobGroup group = jobGroupMap.get(monitorTaskId);
			if (group == null) {
				group = new JobGroup(monitorTaskId);
				jobGroupMap.put(monitorTaskId, group);
			}

			task.setDataSource(dataSource);
			Job job = new Job(task);

			group.addJob(job);
		}

		return jobGroupMap.values();
	}

}
