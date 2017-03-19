package cn.uway.ubp.monitor.indicator.filter;

import java.sql.Connection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.ubp.monitor.dao.AlarmClearDAO;
import cn.uway.ubp.monitor.dao.MonitorTaskDAO;
import cn.uway.ubp.msg.AlarmClearException;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.Exclude;
import cn.uway.util.entity.ExcludeSql;
import cn.uway.util.entity.Filter;
import cn.uway.util.entity.Include;
import cn.uway.util.entity.IncludeSql;
import cn.uway.util.entity.MonitorTask;

/**
 * <pre>
 * Filter创建工厂
 * 根据任务中的filter信息创建具体的Filter对象
 * 如果是闭环任务,则单独添加闭环过滤器(做为第一个过滤规则)
 * 
 * @author chenrongqiang @ 2013-6-18
 * </pre>
 */
public class FilterBuilder {

	private static final Logger logger = LoggerFactory
			.getLogger(FilterBuilder.class);
	
	public static final int ALARM_MONITOR_FLAG = 0;
	
	public static final int ALERM_CLEAR_FLAG = 1;

	public static final int ALERM_REJECT_FLAG = 2;

	/**
	 * <pre>
	 * 解析任务过滤器信息 并且创建具体的filter对象
	 * 
	 * @return List<Filter>
	 * @throws Exception
	 * </pre>
	 */
	public static List<IFilter> buildFilters(Connection alarmConn, Connection taskConn, MonitorTask monitorTask,
			DataSource dataSource, Map<String, List<Date>> dateMap)
			throws AlarmClearException, Exception {
		// 解析xml
		List<IFilter> filterList = new LinkedList<IFilter>();
		IFilter iFilter = null;

		// change:shig gang,闭环要用监控字段，不能用数据源索引字段
		String indexField = monitorTask.getMonitorField(); // FieldInfoDAO.getInstance().getIndexFieldByTaskId(monitorTask.getTaskId());
		if (StringUtils.isBlank(indexField)) {
			logger.debug("任务没有索引字段，TaskId：" + monitorTask.getTaskId());
			throw new AlarmClearException("任务没有索引字段，TaskId："
					+ monitorTask.getTaskId());
		}
		/**
		 * 如果是闭环任务,且启用了闭环免疫网元配置,则取出需要过滤的网元 构造一个闭环过滤器,添加至过滤器列表中
		 */
		if (monitorTask.getAlarmClear() == ALERM_CLEAR_FLAG) {
			try {
				/**
				 * 取出需要闭环的网元信息,加入闭环过滤器 如果没有,闭环过滤器正常运行,但所有数据都将被过滤掉
				 */
				List<String> alarmClearIdList = AlarmClearDAO.getInstance()
						.getAlarmClearIdList(alarmConn, monitorTask.getMonitorTaskId(),
								monitorTask.getCurrMonitorTime());

				if (alarmClearIdList.isEmpty()) {
					try {
						// 判断是否还有可运行的任务
						boolean checkRet = MonitorTaskDAO.getInstance()
								.checkTheMonitorHasRunningTask(
										taskConn, monitorTask.getTaskId());
						// 确认没有可运行的告警任务后，才可以删除闭环告警任务
						// 在现网中出现了一个monitor_task_id 下的告警任务中既有is_delete = 0 又有
						// is_delete = 1 的，所以在
						// 清除闭环任务时候需先确认一下是否还有可以运行的告警任务
						if (!checkRet) {
							boolean b = MonitorTaskDAO.getInstance().del(
									taskConn, monitorTask.getTaskId());
							if (b) {
								logger.debug("告警清除任务{}满足注销条件，已删除",
										monitorTask.getTaskId());
							}
						} else {
							logger.debug("发现还有可运行的告警任务，告警清除任务{}不满足注销条件，未删除",
									monitorTask.getTaskId());
						}
					} catch (Exception e) {
						logger.error("告警清除任务{}删除失败", monitorTask.getTaskId());
					}
				}

				iFilter = new AlarmCleanFilter(indexField, alarmClearIdList);
				filterList.add(iFilter);
			} catch (Exception e) {
				throw new AlarmClearException("创建告警清除网元过滤器失败", e);
			}
		}

		// 驳回任务
		/**
		// 驳回的逻辑，移至IndicatorRunner.execute()中处理，判断时间，如果不满足闭环，看事件产生时间是否在回单时间之后．如不是，则不产生驳回事件
		if (monitorTask.getAlarmClear() == ALERM_REJECT_FLAG) {
			try {
				List<ReplyTask> replyTaskList = RejectDAO.getInstance()
						.getReplyTaskList(monitorTask.getMonitorTaskId(),
								monitorTask.getCurrMonitorTime());

				String filedName = dataSource.getDbSourceInfo()
						.getTimeFieldName();

				iFilter = new RejectFilter(monitorTask.getCurrMonitorTime(),
						filedName, replyTaskList, dateMap);
				filterList.add(iFilter);

			} catch (Exception e) {
				throw new AlarmClearException("创建驳回告警过滤器失败", e);
			}

		}*/

		// 未配置过滤
		Filter filter = monitorTask.getFilter();
		if (filter == null)
			return filterList;

		List<Include> includeList = filter.getIncludeList();
		if (includeList != null && !includeList.isEmpty()) {
			for (Include include : includeList) {
				iFilter = new IncludFilter(include.getField(),
						include.getValue(), include.getPrototype());
				filterList.add(iFilter);
			}
		}

		List<Exclude> excludeList = filter.getExcludeList();
		if (excludeList != null && !excludeList.isEmpty()) {
			for (Exclude exclude : excludeList) {
				iFilter = new ExcludFilter(exclude.getField(),
						exclude.getValue(), exclude.getPrototype());
				filterList.add(iFilter);
			}
		}

		List<IncludeSql> includeSqlList = filter.getIncludeSqlList();
		if (includeSqlList != null && !includeSqlList.isEmpty()) {
			for (IncludeSql includeSql : includeSqlList) {
				iFilter = new IncludeSqlFilter(includeSql.getConnectionId(),
						includeSql.getField(), includeSql.getValue(),
						includeSql.getPrototype());
				filterList.add(iFilter);
			}
		}

		List<ExcludeSql> excludeSqlList = filter.getExcludeSqlList();
		if (excludeSqlList != null && !excludeSqlList.isEmpty()) {
			for (ExcludeSql excludeSql : excludeSqlList) {
				iFilter = new ExcludeSqlFilter(excludeSql.getConnectionId(),
						excludeSql.getField(), excludeSql.getValue(),
						excludeSql.getPrototype());
				filterList.add(iFilter);
			}
		}

		return filterList;
	}
}
