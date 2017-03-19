package cn.uway.uport.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.common.DAO;
import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.uport.context.ApplicationContext;
import cn.uway.uport.context.Configuration;
import cn.uway.uport.context.DbPoolManager;
import cn.uway.util.DateGranularityUtil;
import cn.uway.util.MonitorUtil;
import cn.uway.util.MonitorUtil.MonitorPeriodTimeInfo;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.DbSourceField;
import cn.uway.util.entity.DbSourceInfo;
import cn.uway.util.entity.DbSourceTable;
import cn.uway.util.entity.Filter;
import cn.uway.util.entity.Holiday;
import cn.uway.util.entity.MonitorTask;
import cn.uway.util.entity.Offset;
import cn.uway.util.entity.Range;
import cn.uway.util.entity.Rule;

/**
 * 监控任务DAO
 * 
 * @author liuchao 2013年11月7日
 */
public class MonitorTaskDAO extends DAO {

	private static final MonitorTaskDAO DAO = new MonitorTaskDAO();

	private Map<Long, Set<String>> dbSourcePKFieldMap = new HashMap<Long, Set<String>>();

	private static final Logger logger = LoggerFactory
			.getLogger(MonitorTaskDAO.class);

	public static MonitorTaskDAO getInstance() {
		return DAO;
	}

	/**
	 * <pre>
	 * 添加新任务
	 * 
	 * @param taskList 新任务信息
	 * @return 返回新任务Id
	 * @throws SQLException 添加时出错
	 * </pre>
	 */
	public void add(List<MonitorTask> taskList) throws SQLException {
		Connection conn = null;

		try {
			conn = DbPoolManager
					.getConnection(ApplicationContext.TASK_DATABASE);
			conn.setAutoCommit(false);

			add(conn, taskList);

			conn.commit();
		} catch (SQLException e) {
			if (conn != null)
				conn.rollback();

			throw e;
		} finally {
			DatabaseUtil.close(conn);
		}
	}

	/**
	 * <pre>
	 * 检查任务Id是否已经存在
	 * 
	 * @param monitorTaskId 任务Id
	 * @param callerId 调用者Id
	 * @return 任务Id是否存在
	 * @throws SQLException
	 * </pre>
	 */
	public boolean exists(String monitorTaskId, int callerId)
			throws SQLException {
		Connection conn = null;

		try {
			conn = DbPoolManager
					.getConnection(ApplicationContext.TASK_DATABASE);
			boolean existsFlag = exists(conn, monitorTaskId, callerId);

			return existsFlag;
		} finally {
			DatabaseUtil.close(conn);
		}
	}

	/**
	 * <pre>
	 * 检查任务Id是否已经存在
	 * 判断逻辑：不能存在相同的monitor_task_id 并且没有被删除
	 * 
	 * @param conn Connection
	 * @param monitorTaskId 任务Id
	 * @param callerId 调用者Id
	 * @return 任务Id是否存在
	 * @throws SQLException
	 * </pre>
	 */
	private boolean exists(Connection conn, String monitorTaskId, long callerId)
			throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select count(0) from ubp_monitor_cfg_task where monitor_task_id=? and caller_id=? and is_deleted = 0";
		int count = 0;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, monitorTaskId);
			pstmt.setLong(2, callerId);

			log(logger, conn, sql, monitorTaskId, callerId);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getInt(1);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return count > 0;
	}

	/**
	 * <pre>
	 * 添加新任务
	 * 
	 * @param conn Connection
	 * @param taskList 新任务信息
	 * @return 新任务Id
	 * @throws SQLException 合并SQL语句字段失败
	 * </pre>
	 */
	private void add(Connection conn, List<MonitorTask> taskList)
			throws SQLException {
		MonitorTask task = taskList.get(0);
		String monitorTaskId = task.getMonitorTaskId();
		long callerId = task.getCallerId();
		Date currMonitorTime = task.getCurrMonitorTime();

		// STEP:检查MonitorTaskId是否已存在
		boolean exists = exists(conn, monitorTaskId, callerId);

		if (exists)
			throw new IllegalArgumentException(
					"添加失败,MonitorTaskId已存在,MonitorTaskId：" + monitorTaskId
							+ ", CallerId:" + callerId);

		// STEP:添加数据源信息
		DataSource dataSource = task.getDataSource();

		String granularity = dataSource.getGranularity().toString();

		// 计算CurrDataTime
		Timestamp currDataTime = computeDate(currMonitorTime, granularity,
				taskList);

		// STEP:添加(合并)数据源
		long dataSourceId = addDataSource(conn, dataSource, currDataTime);

		// STEP:添加监控任务
		addMonitorTask(conn, taskList, dataSourceId);
	}

	/**
	 * <pre>
	 * 计算数据源时间
	 * 循环用FirstMonitTime减去各分析周期粒度
	 * 然后取时间最久远的
	 * 
	 * @param firstMonitorTime 第1次监控时间
	 * @param granularity 数据源粒度
	 * @param taskList 监控任务列表
	 * @return 返回根据所有规则计算出来的最早的时间
	 * </pre>
	 */
	private Timestamp computeDate(Date firstMonitorTime, String granularity,
			List<MonitorTask> taskList) {
		Date currDataTime = null;

		for (MonitorTask task : taskList) {
			Date _currDataTime = computeDate(task, firstMonitorTime,
					granularity);

			// 如果当前计算出来的时间"早于"先前计算出来的时间,则替换
			if (currDataTime == null
					|| _currDataTime.compareTo(currDataTime) < 0) {
				currDataTime = _currDataTime;
			}
		}

		return new Timestamp(currDataTime.getTime());
	}

	/**
	 * <pre>
	 * 计算子任务的数据时间
	 * 
	 * @param task 子任务
	 * @param firstMonitorTime 第1次监控时间
	 * @param dsGranularity 数据源粒度
	 * @return 返回数据时间
	 * </pre>
	 */
	private Date computeDate(MonitorTask task, Date firstMonitorTime,
			String dsGranularity) {
		Rule rule = task.getRule();
		String monitorPeriodUnit = rule.getPeriodInfo().getMonitorPeriod()
				.getUnit().toString();
		Integer holidayPolicy = null;
		Filter filter = task.getFilter();
		if (filter != null) {
			Holiday holiday = filter.getHoliday();
			if (holiday != null) {
				holidayPolicy = holiday.getPolicy();
			}
		}

		// 数据范围
		List<Range> rangeList = task.getExpression().getDataRange()
				.getRangeList();
		Map<String, List<Offset>> rangeOffsets = MonitorUtil.offsets(rangeList);

		// 运算时间点
		List<MonitorPeriodTimeInfo> monitortimesInfo = null;

		boolean enableHoliday = Configuration
				.getBoolean(Configuration.HOLIDAY_ENABLE);

		// 非频次运算
		if (rangeList.size() > 1) {
			// 最大的时间偏移数
			int maxOffsetTime = MonitorUtil.getMaxOffsetTimers(rangeOffsets);
			int minuOfMonitorPeriod = DateGranularityUtil
					.minutes(monitorPeriodUnit);
			// 分析个数 = 最大的偏移时间/每个监控周期单位时间粒度
			int nAnalsysisNum = maxOffsetTime / minuOfMonitorPeriod;

			// FIXME 这里有bug
			monitortimesInfo = MonitorUtil.getMonitorTimes(monitorPeriodUnit,
					nAnalsysisNum, firstMonitorTime, rule, holidayPolicy,
					dsGranularity, enableHoliday,false,false);
		} else {
			// 频次运算
			String analysisPeriodUnit = rule.getPeriodInfo()
					.getAnalysisPeriod().getUnit().toString();
			int analysisPeriodNum = rule.getPeriodInfo().getAnalysisPeriod()
					.getPeriodNum();
			monitortimesInfo = MonitorUtil.getMonitorTimes(analysisPeriodUnit,
					analysisPeriodNum, firstMonitorTime, rule, holidayPolicy,
					dsGranularity, enableHoliday,false,false);
		}

		if (monitortimesInfo.size() > 0) {
			// monitorPeriodInfo 的监控周期时间点是由高至低存放的(所以取最后一个)
			MonitorPeriodTimeInfo monitorPeriodInfo = monitortimesInfo
					.get(monitortimesInfo.size() - 1);
			// 数据时间点是低至高存放的(所以取第1个)
			return monitorPeriodInfo.dataTimes.get(0);
		} else {
			return null;
		}
	}

	/**
	 * <pre>
	 * 添加(合并)数据源
	 * 如果数据源已存在,则合并.
	 * 		如果字段有变化,需要新添加字段,则有可能需要更新数据源的数据时间
	 * 		当前数据源时间早于当前监控任务需要的时间，则不需要更新（SQL中实现）
	 * 如果数据源不存在
	 * 		添加数据源
	 * 		添加数据源表
	 * 		添加数据源表字段
	 * 
	 * @param conn Connection
	 * @param dataSource 数据源
	 * @param currDataTime 经过运算的当前数据时间
	 * @return
	 * @throws SQLException 合并Sql语句字段失败
	 * </pre>
	 */
	private long addDataSource(Connection conn, DataSource dataSource,
			Timestamp currDataTime) throws SQLException {
		// 取数据源Id
		String granularity = dataSource.getGranularity().toString();
		String neLevel = dataSource.getNeLevel();
		String netType = dataSource.getNetType();
		boolean isLogDrive = dataSource.isLogDrive();
		/**
		 * <pre>
		 * 	temporary change:sg
		 * 	date:2014-4-29
		 * 	explain: 	容强提需求， 如果日志驱动和非日志驱动的数据源不要合并，
		 * 				所以在这里，查找是否符合合并规则时，带上当前数据源的isLogDrive标识，
		 * 				让其只查出isLogDrive相同的数据源
		 * </pre>
		 */
		List<DbSourceTable> dsTableList = getDataSourceTables(conn,
				granularity, neLevel, netType, isLogDrive);
		DbSourceInfo dbSourceInfo = dataSource.getDbSourceInfo();
		List<DbSourceTable> currTableList = dbSourceInfo.getDbSourceTableList();

		// 数据源存在,比较数据源表
		if (dsTableList != null && dsTableList.size() > 0) {
			/**
			 * <pre>
			 * chris @2014-01-03 以数据源表中是否包含Sql区分联通/电信，联通/电信的合并逻辑不同
			 * 联通：相同的表数量、表名、表连接Id、多表关联关系
			 * 电信：合并基于一个前提，所有字段名是唯一的。
			 * 合并不以提供的表名（视图名）为依据，查数据源中完全包含当前数据表中字段的表做合并。
			 * 先取所有表，遍历->取表字段->比较字段，完全包含则合并，并返回该数据源表的数据源Id
			 * 
			 * chris @2014-05-16 经与杨少江、任广讨论，需要添加对表名的判断，以表名加字段确定唯一性
			 * 
			 * chris @2014-07-06 经与杨少江，任广、谢韶光、许瑞林讨论决定的修改，具体见修改方法注释
			 * </pre>
			 */
			boolean flag = hasSqlInDbTable(currTableList);
			if (flag) {
				// 电信（实际应用中只有一个数据源表）
				/**
				 * 0801 在版本升级中出现了兼容电信老数据的问题， 为了保证最小改动，只在电信业务逻辑处修改代码，数据源查询逻辑不动
				 * UBP_MONITOR_DATASOURCE_TABLE 添加 ADDTIME字段区分新老数据
				 */

				DbSourceTable currTable = currTableList.get(0);

				for (DbSourceTable dsTable : dsTableList) {
					if (dsTable.getAddTime() != null) {

						if (dsTable.getName().equalsIgnoreCase(
								currTable.getName())
								&& dsTable.getConnectionId() == currTable
										.getConnectionId()) {
							/**
							 * 1、先删除表中所有字段 2、再添加字段 3、更新SQL 4、更新数据源时间
							 */
							long tableId = dsTable.getTableId();

							// 1、先删除表中所有字段
							delDataSourceFieldByTableId(conn, tableId);

							// 2、再添加字段
							List<DbSourceField> fieldList = currTableList
									.get(0).getFieldList();
							int isIndex, isExport;
							for (DbSourceField field : fieldList) {
								isIndex = field.isIndex() ? 1 : 0;
								isExport = field.isExport() ? 1 : 0;
								addDataSourceField(conn, tableId,
										field.getName(), isIndex, isExport);
							}

							// 3、更新SQL
							updateTableSqlByTableId(conn, tableId,
									currTable.getSql());
							
							/**
							 * 2015.04.22，Chris，修改bug，电信端如果更新数据源后，没有更新数据源时间，导致获取序列化数据文件失败
							 */
							// 4、更新数据源时间
							updateDataTimeByDataSourceId(conn, dsTable.getDataSourceId(),
									currDataTime);

							return dsTable.getDataSourceId();

							/*
							 * // 注意：取字段这里其实当运行的任务（数据源）较多后，会比较影响性能！！！
							 * Set<String> fieldSet =
							 * getDataSourceFieldByTableId(conn,
							 * dsTable.getTableId()); //
							 * 循环当前下发的数据源表（对于这种情况，实际只有一张表） CYCLE_FLAG : for
							 * (DbSourceTable currTable : currTableList) { if
							 * (!dsTable.getName().equalsIgnoreCase(currTable
							 * .getName())) break CYCLE_FLAG;
							 * 
							 * List<DbSourceField> currFieldList =
							 * currTable.getFieldList(); boolean _flag =
							 * compareDataSourceField(fieldSet, currFieldList);
							 * if (!_flag) { // 表字段不符合合并规则，则退出循环，比较下一张表 break
							 * CYCLE_FLAG; } else { //
							 * 表字段符合合并规则，退出方法，返回当前数据源表的数据源Id return
							 * dsTable.getDataSourceId(); } }
							 */
						}
					}
				}
			} else {
				// 联通
				List<DbSourceTable> dataSourceTableList = compareDataSourceTable(
						conn, currTableList, dsTableList,
						dbSourceInfo.getTableRelation());
				if (dataSourceTableList != null) {
					long dataSourceId = merge(conn, currTableList,
							dataSourceTableList);
					updateDataTimeByDataSourceId(conn, dataSourceId,
							currDataTime);
					return dataSourceId;
				}
			}
		}

		/*
		 * 数据源不存在,添加数据源,表,字段 数据源的DataTime字段,初始值与任务的当前数据时间一致
		 */
		dataSource.setDataTime(currDataTime);
		long dataSourceId = addDataSourceInfo(conn, dataSource, currTableList);

		return dataSourceId;
	}

	/**
	 * <pre>
	 * 比较dbSourceFieldSet中是否完全包含currFieldList中的字段
	 * 
	 * @param dbSourceFieldSet
	 * @param currFieldList
	 * @return 返回true为完全包含，否则返回false
	 * </pre>
	 */
	/*
	 * private boolean compareDataSourceField(Set<String> dbSourceField,
	 * List<DbSourceField> currFieldList) { for (DbSourceField field :
	 * currFieldList) { if (!dbSourceField.contains(field.getName())) return
	 * false; }
	 * 
	 * return true; }
	 */

	/**
	 * <pre>
	 * 数据源表中是否包含Sql 用以区分联通/电信的数据源合并逻辑
	 * 
	 * @param tableList
	 * @return 返回true说明至少有一个数据源表包含Sql（sql节点不为空），否则返回false
	 * </pre>
	 */
	private boolean hasSqlInDbTable(List<DbSourceTable> tableList) {
		for (DbSourceTable table : tableList) {
			if (StringUtils.isNotBlank(table.getSql()))
				return true;
		}

		return false;
	}

	/**
	 * <pre>
	 * 获取数据源表列表
	 * 
	 * @param conn Connection
	 * @param granularity 数据源粒度
	 * @param neLevel 数据源级别
	 * @param netType 网络类型
	 * @return
	 * @throws SQLException
	 * </pre>
	 */
	private List<DbSourceTable> getDataSourceTables(Connection conn,
			String granularity, String neLevel, String netType,
			boolean isLogDrive) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select distinct dst.table_id,dst.table_name,dst.connection_id,ds.id datasource_id ,dst.addtime"
				+ " from ubp_monitor_datasource ds,ubp_monitor_datasource_table dst"
				+ " where ds.id = dst.datasource_id and ds.granularity=? and ds.ne_level=? and ds.net_type=? and ds.is_log_drive=?";
		List<DbSourceTable> dsTableList = new ArrayList<>();

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, granularity);
			pstmt.setString(2, neLevel);
			pstmt.setString(3, netType);
			pstmt.setInt(4, (isLogDrive ? 1 : 0));

			log(logger, conn, sql, granularity, neLevel, netType);

			rs = pstmt.executeQuery();
			while (rs.next()) {
				DbSourceTable dsTable = new DbSourceTable(rs.getLong(1),
						rs.getString(2), rs.getLong(3), rs.getLong(4));
				dsTable.setAddTime(rs.getTimestamp(5));
//				getDataSourceTablesPKField(conn, dsTable);
				dsTableList.add(dsTable);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return dsTableList;
	}

	/**
	 * <pre>
	 * 更新数据源时间
	 * 
	 * @param conn Connection
	 * @param tableId 数据源表Id
	 * @param sql sql
	 * @throws SQLException
	 * </pre>
	 */
	private void updateTableSqlByTableId(Connection conn, long tableId,
			String newSql) throws SQLException {
		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_datasource_table set table_sql=? where table_Id=?";

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, newSql);
			pstmt.setLong(2, tableId);

			log(logger, conn, sql, tableId, newSql);

			pstmt.executeUpdate();
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 更新数据源时间
	 * 
	 * @param conn Connection
	 * @param dataSourceId 数据源Id
	 * @param currDataTime 数据源时间
	 * @throws SQLException
	 * </pre>
	 */
	private void updateDataTimeByDataSourceId(Connection conn,
			long dataSourceId, Timestamp currDataTime) throws SQLException {
		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_datasource set data_time=? where id=? and data_time>?";

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setTimestamp(1, currDataTime);
			pstmt.setLong(2, dataSourceId);
			pstmt.setTimestamp(3, currDataTime);

			log(logger, conn, sql, currDataTime, dataSourceId, currDataTime);

			pstmt.executeUpdate();
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * 从数据库中查看相同表中PK主键字段
	 * 
	 * <p>
	 * 2015-06-10 sunt
	 * <p>
	 * 现象：吉林反应uport修改任务计划很慢，分析后发现一般每个计划包含8个监控任务，每个监控任务耗时1.5分钟，共耗时10分钟以上。
	 * <p>
	 * 原因：getDataSourceTables方法中会循环调用该方法，循环3500次，就是1分多钟了。
	 * <p>
	 * 根结：ubp_monitor_datasource_field表有近6w数据，无索引、无主键，查询效率极低，每次查询在0.040秒左右。
	 * <p>
	 * 处理方式：不在getDataSourceTables中调用，在compareDataSourceTable中调用。
	 * 
	 * @throws SQLException
	 */
	public void getDataSourceTablesPKField(Connection conn,
			long tableId) throws SQLException {
//			DbSourceTable dsTable) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String getPKFieldSql = " select  field_name  from ubp_monitor_datasource_field where table_id = ? and  is_index = 1";
		Set<String> dbSourcePKFieldSet = new HashSet<String>();
		try {
			pstmt = conn.prepareStatement(getPKFieldSql);
			pstmt.setLong(1, tableId);
//			pstmt.setLong(1, dsTable.getTableId());
			rs = pstmt.executeQuery();
			while (rs.next()) {
				dbSourcePKFieldSet.add(rs.getString(1).toUpperCase());
			}
			if (dbSourcePKFieldSet.size() > 0)
				dbSourcePKFieldMap
						.put(tableId, dbSourcePKFieldSet);
//						.put(dsTable.getTableId(), dbSourcePKFieldSet);
		} catch (Exception e) {
			err(logger, conn, getPKFieldSql, e);
			throw e;
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}
	}

	/**
	 * <pre>
	 * 合并数据源
	 * @param conn Connection
	 * @param tableList 新监控任务增加的表
	 * @param dataSourceTableList 当前数据源表
	 * @return 数据源Id
	 * @throws SQLException 合并Sql语句字段失败
	 * </pre>
	 */
	private Long merge(Connection conn, List<DbSourceTable> tableList,
			List<DbSourceTable> dataSourceTableList) throws SQLException {
		// 数据源相同(表数量相等以及表名相同),合并
		Long dataSourceId = null;
		boolean changeFlag = false;
		// 数据源表完全相同,视为同一个数据源,合并字段
		for (DbSourceTable table : tableList) {
			// 内层循环是为了找到相同表(表名与连接Id一致)的表Id
			for (DbSourceTable dst : dataSourceTableList) {
				/**
				 * 记下数据源Id,做为返回值
				 */
				if (dataSourceId == null) {
					dataSourceId = dst.getDataSourceId();
				}
				if (table.getName().equals(dst.getName())
						&& table.getConnectionId() == dst.getConnectionId()) {
					long tableId = dst.getTableId();
					boolean currChangeFlag = mergeDataSourceField(conn,
							tableId, table.getFieldList());
					/**
					 * 20130918,Chris 如果有新加字段,则可能需要对数据源时间进行更新
					 */
					if (currChangeFlag && !changeFlag) {
						changeFlag = true;
					}
					break;
				}
			}
		}

		return dataSourceId;
	}

	/**
	 * <pre>
	 * 比较数据库中数据源表是否与当前下发任务的数据源表完全一致
	 * 1：先按数据源分组
	 * 2：每组分别与现表比较,表数量相等,以及每个表名与连接Id完全相同,且多表的关联关系相同
	 * 3：若最终未找到，则返回null
	 * @param conn Connection
	 * @param currTableList 当前下发任务的数据源表
	 * @param dsTableList 数据库中数据源表
	 * @param tableRelation 当前下发的多表关联关系
	 * @return 完全一致则返回该组集合，否则返回null
	 * @throws SQLException
	 * </pre>
	 */
	private List<DbSourceTable> compareDataSourceTable(Connection conn,
			List<DbSourceTable> currTableList, List<DbSourceTable> dsTableList,
			String tableRelation) throws SQLException {
		/**
		 * 存在Map中,便于查找 因为先比较了表数量是否一致，所以将数据源中表存入Map还是当前任务的表存在Map不重要
		 */
		Map<String, Long> tmp = new HashMap<String, Long>();
		Set<String> currentPKFieldSet = new HashSet<String>();
		for (DbSourceTable table : currTableList) {
			tmp.put(table.getName(), table.getConnectionId());
			if (table.getFieldList() != null && table.getFieldList().size() > 0) {
				for (DbSourceField dbSourceField : table.getFieldList()) {
					if (dbSourceField.isIndex()) {
						currentPKFieldSet.add(dbSourceField.getName());
					}
				}
			}
		}

		for (DbSourceTable table : currTableList) {
			tmp.put(table.getName(), table.getConnectionId());
		}
		
		// 2015-06-10 sunt ，见getDataSourceTablesPKField方法注释
		// 实例化dbSourcePKFieldMap
		for (DbSourceTable table : dsTableList) {
			getDataSourceTablesPKField(conn, table.getTableId());
		}

		// STEP1：按数据源分组
		Map<Long, List<DbSourceTable>> tableMap = new HashMap<>();
		for (DbSourceTable dsTable : dsTableList) {
			List<DbSourceTable> dbTableList = tableMap.get(dsTable
					.getDataSourceId());
			if (dbTableList == null) {
				dbTableList = new ArrayList<>();
				tableMap.put(dsTable.getDataSourceId(), dbTableList);
			}

			dbTableList.add(dsTable);
		}

		// 　STEP2:每组分别与现表比较
		for (List<DbSourceTable> dataSourceTableList : tableMap.values()) {
			// 如果数量都不相等，中止本次循环，进入下一组比较
			if (currTableList.size() != dataSourceTableList.size())
				continue;

			// 相同次数记数器
			int counter = 0;

			for (DbSourceTable table : dataSourceTableList) {
				if (StringUtils.isNotBlank(table.getSql())) {
					break;
				}

				if (!tmp.containsKey(table.getName())) {
					continue;
				}

				long value = tmp.remove(table.getName());
				if (value != table.getConnectionId()) {
					continue;
				}

				counter++;
			}

			// STEP3：相同次数与表数量一致，且多表的关联关系相同，则说明表完全一样
			if (counter == dataSourceTableList.size()) {
				if (counter > 1) {
					Long datasourceId = dataSourceTableList.get(0)
							.getDataSourceId();
					String tableRelationOfDataSource = getTableRelationById(
							conn, datasourceId);
					// TODO 判断多表关联关系应更细致（拆分条件，逐个判断；不区分大小写）(-)
					if (!StringUtils.equalsIgnoreCase(tableRelation,
							tableRelationOfDataSource)) {
						continue;
					}
					return dataSourceTableList;
				}

				// 判断数据中数据源表中主键个数和主键是否相同
				if (dbSourcePKFieldMap.size() > 0) {
					for (DbSourceTable table : dsTableList) {
						Set<String> tmpDbSourcePKFieldSet = dbSourcePKFieldMap
								.get(table.getTableId());
						if (tmpDbSourcePKFieldSet != null) {
							if (comparePKField(tmpDbSourcePKFieldSet,
									currentPKFieldSet)) {
								// 如果主键个数不同却又是同一个表，表现为在数据库中是不同的数据源，要找到真正的数据源这里要重新设置下数据源ID
								dataSourceTableList.get(0).setDataSourceId(
										table.getDataSourceId());
								return dataSourceTableList;
							} else {
								return null;
							}
						}
					}
				}
			}
		}

		return null;
	}

	/**
	 * <pre>
	 * 添加数据源表关联关系
	 * @param conn Connection
	 * @param dataSourceId 数据源Id
	 * @return
	 * @throws SQLException
	 * </pre>
	 */
	private String getTableRelationById(Connection conn, long dataSourceId)
			throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select condition from ubp_monitor_datasource where id=?";
		String relation = null;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, dataSourceId);

			log(logger, conn, sql, rs, dataSourceId);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				relation = rs.getString(1);
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return relation;
	}

	/**
	 * 判断当前任务主键和数据库中任务主键是否相同
	 * 
	 * @param dbSourcePKFieldSet
	 * @param currentPKFieldSet
	 * @return
	 * @throws SQLException
	 */
	private boolean comparePKField(Set<String> dbSourcePKFieldSet,
			Set<String> currentPKFieldSet) throws SQLException {
		if (dbSourcePKFieldSet == null || currentPKFieldSet == null) {
			throw new SQLException(
					"comparePKField() 传入的 dbSourcePKFieldSet 为空或currentPKFieldSet为空");
		}
		if (currentPKFieldSet.size() == dbSourcePKFieldSet.size()) {
			for (String pkName : currentPKFieldSet) {
				if (!dbSourcePKFieldSet.contains(pkName.toUpperCase()))
					return false;
			}
		} else {
			return false;
		}

		return true;
	}

	/**
	 * <pre>
	 * 循环添加监控子任务 总体分为2个步骤:
	 * 1,添加监控任务
	 * 2,添加任务的所有字段
	 * 
	 * @param conn Connection
	 * @param taskList 监控任务
	 * @param dataSourceId 数据源Id
	 * @param monitorTaskId 监控任务Id(前端接口调用传入)
	 * @throws SQLException
	 * </pre>
	 */
	private void addMonitorTask(Connection conn, List<MonitorTask> taskList,
			long dataSourceId) throws SQLException {
		for (MonitorTask task : taskList) {
			// 如果Job配置中有覆盖运行周期,则替换监控任务默认的运行周期
			task.setDataSourceId(dataSourceId);
			long taskId = addMonitorTask(conn, task);

			// 填充任务字段
			Set<String> fieldSet = task.getDataSource().getFieldSet();
			addMonitorTaskField(conn, taskId, fieldSet);
		}
	}

	/**
	 * <pre>
	 * 添加监控任务
	 * 
	 * @param conn Connection
	 * @param task 监控任务实体类
	 * @return 监控任务TaskId
	 * @throws SQLException
	 * </pre>
	 */
	private long addMonitorTask(Connection conn, MonitorTask task)
			throws SQLException {
		PreparedStatement pstmtForSeq = null;
		PreparedStatement pstmtForAdd = null;
		ResultSet rs = null;
		String sqlForSeq = "select ubp_cfg_task_seq.nextval from dual";
		String sqlForAdd = "insert into ubp_monitor_cfg_task(task_id, task_name, monitor_task_id, caller_id, period_num, period_unit,"
				+ "datasource_id, curr_monitor_time, end_monitor_time, filter, city_id, monitor_field, is_alarm_clear, pi_name,"
				+ "pi_expr_description, t_id, key_index_type, is_used, expression_info, period_info, top_info, alarm_level_info,"
				+ "rule_description,memo)"
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		long taskId;

		try {
			// STEP1：取sequence
			pstmtForSeq = conn.prepareStatement(sqlForSeq);

			log(logger, conn, sqlForSeq);

			rs = pstmtForSeq.executeQuery();
			rs.next();
			taskId = rs.getLong(1);

			// STEP2：添加数据源表

			int isAlarmClear = task.getAlarmClear();

			if (task.isReject())
				isAlarmClear = 2; // 标记为驳回任务

			String periodUnit = task.getPeriodUnit().toString();
			int tId = task.gettId() == null ? 0 : task.gettId();
			int keyIndexType = task.getKeyIndexType() == null ? 0 : task
					.getKeyIndexType();
			int isUsed = task.isUsed() ? 1 : 0;
			if (!task.isJobEnable())
				isUsed = 0;

			pstmtForAdd = conn.prepareStatement(sqlForAdd);
			pstmtForAdd.setLong(1, taskId);
			pstmtForAdd.setString(2, task.getTaskName());
			pstmtForAdd.setString(3, task.getMonitorTaskId());
			pstmtForAdd.setLong(4, task.getCallerId());
			pstmtForAdd.setInt(5, task.getPeriodNum());
			pstmtForAdd.setString(6, task.getPeriodUnit().toString());
			pstmtForAdd.setLong(7, task.getDataSourceId());
			pstmtForAdd.setTimestamp(8, task.getCurrMonitorTime());
			pstmtForAdd.setTimestamp(9, task.getEndMonitorTime());
			pstmtForAdd.setString(10, task.getFilterContent());
			pstmtForAdd.setInt(11, task.getCityId());
			pstmtForAdd.setString(12, task.getMonitorField());
			pstmtForAdd.setInt(13, isAlarmClear);
			pstmtForAdd.setString(14, task.getPiName());
			pstmtForAdd.setString(15, task.getPiExprDescription());
			pstmtForAdd.setInt(16, tId);
			pstmtForAdd.setInt(17, keyIndexType);
			pstmtForAdd.setInt(18, isUsed);
			pstmtForAdd.setString(19, task.getExpressionContent());
			pstmtForAdd.setString(20, task.getPeriodInfoContent());
			pstmtForAdd.setString(21, task.getTopContent());
			pstmtForAdd.setString(22, task.getAlarmLevelContent());
			pstmtForAdd.setString(23, task.getRuleDescription());
			pstmtForAdd.setString(24, task.getMemo());

			log(logger, conn, sqlForAdd, taskId, task.getTaskName(),
					task.getMonitorTaskId(), task.getCallerId(),
					task.getPeriodNum(), periodUnit, task.getDataSourceId(),
					task.getCurrMonitorTime(), task.getEndMonitorTime(),
					task.getFilterContent(), task.getCityId(),
					task.getMonitorField(), isAlarmClear, task.getPiName(),
					task.getPiExprDescription(), tId, keyIndexType, isUsed,
					task.getExpressionContent(), task.getPeriodInfoContent(),
					task.getTopContent(), task.getAlarmLevelContent(),
					task.getRuleDescription(), task.getMemo());

			pstmtForAdd.executeUpdate();
		} finally {
			DatabaseUtil.close(rs, pstmtForSeq);
			DatabaseUtil.close(pstmtForAdd);
		}

		return taskId;
	}

	/**
	 * <pre>
	 * 添加任务字段列表
	 * 
	 * @param conn
	 * @param taskId
	 * @param fieldSet
	 * @throws SQLException
	 * </pre>
	 */
	private void addMonitorTaskField(Connection conn, long taskId,
			Set<String> fieldSet) throws SQLException {
		PreparedStatement pstmt = null;
		String sql = "insert into ubp_monitor_task_field(task_id, field_name) values(?, ?)";

		try {
			pstmt = conn.prepareStatement(sql);

			for (String field : fieldSet) {
				pstmt.setLong(1, taskId);
				pstmt.setString(2, field);
				pstmt.addBatch();

				log(logger, conn, sql, taskId, field);
			}

			pstmt.executeBatch();
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 添加数据源信息(包括数据源,表,字段)
	 * 
	 * @param conn Connection
	 * @param dataSource
	 * @param tableList
	 * @throws SQLException
	 * </pre>
	 */
	private long addDataSourceInfo(Connection conn, DataSource dataSource,
			List<DbSourceTable> tableList) throws SQLException {
		// STEP:添加数据源
		long dataSourceId = addDataSource(conn, dataSource);

		// STEP:添加数据源表
		addDataSourceTable(conn, dataSourceId, tableList);

		return dataSourceId;
	}

	/**
	 * <pre>
	 * 添加数据源
	 * @param conn Connection
	 * @param dataSource 数据源
	 * @return 数据源Id
	 * @throws SQLException
	 * </pre>
	 */
	private long addDataSource(Connection conn, DataSource dataSource)
			throws SQLException {
		PreparedStatement pstmtForSeq = null;
		PreparedStatement pstmtForAdd = null;
		ResultSet rs = null;
		String sqlForSeq = "select ubp_cfg_datasource_seq.nextval from dual";
		String sqlForAdd = "insert into ubp_monitor_datasource(id, condition, granularity, data_time, data_time_delay, ne_level,"
				+ " net_type, master_table, time_field, time_field_type, type, is_log_drive) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		long dataSourceId;

		try {
			// STEP1：取sequence
			pstmtForSeq = conn.prepareStatement(sqlForSeq);

			log(logger, conn, sqlForSeq);

			rs = pstmtForSeq.executeQuery();
			rs.next();
			dataSourceId = rs.getLong(1);

			// STEP2：添加数据源表
			int isLogDrive = dataSource.isLogDrive() ? 1 : 0;
			String granularity = dataSource.getGranularity().toString();
			DbSourceInfo dbSourceInfo = dataSource.getDbSourceInfo();
			pstmtForAdd = conn.prepareStatement(sqlForAdd);
			pstmtForAdd.setLong(1, dataSourceId);
			pstmtForAdd.setString(2, dbSourceInfo.getTableRelation());
			pstmtForAdd.setString(3, granularity);
			pstmtForAdd.setTimestamp(4, dataSource.getDataTime());
			pstmtForAdd.setInt(5, dbSourceInfo.getDataDelay());
			pstmtForAdd.setString(6, dataSource.getNeLevel());
			pstmtForAdd.setString(7, dataSource.getNetType());
			pstmtForAdd.setString(8, dbSourceInfo.getTimeFieldTable());
			pstmtForAdd.setString(9, dbSourceInfo.getTimeFieldName());
			pstmtForAdd.setString(10, dbSourceInfo.getTimeFieldType());
			pstmtForAdd.setInt(11, dataSource.getType());
			pstmtForAdd.setInt(12, isLogDrive);

			log(logger, conn, sqlForAdd, dataSourceId,
					dbSourceInfo.getTableRelation(), granularity,
					dataSource.getDataTime(), dbSourceInfo.getDataDelay(),
					dataSource.getNeLevel(), dataSource.getNetType(),
					dbSourceInfo.getTimeFieldTable(),
					dbSourceInfo.getTimeFieldName(),
					dbSourceInfo.getTimeFieldType(), dataSource.getType(),
					isLogDrive);

			pstmtForAdd.executeUpdate();
		} finally {
			DatabaseUtil.close(rs, pstmtForSeq);
			DatabaseUtil.close(pstmtForAdd);
		}

		return dataSourceId;
	}

	/**
	 * <pre>
	 * 添加数据源表(包括数据源表,字段)
	 * 
	 * @param conn Connection
	 * @param datasourceId 数据源Id
	 * @param tableList 数据源表列表
	 * @throws SQLException 添加数据源表失败时
	 * </pre>
	 */
	private void addDataSourceTable(Connection conn, long dataSourceId,
			List<DbSourceTable> tableList) throws SQLException {
		for (DbSourceTable table : tableList) {
			long connectionId = table.getConnectionId();
			String tableName = table.getName();
			String tableSql = table.getSql();

			Long tableId = addDataSourceTable(conn, dataSourceId, connectionId,
					tableName, tableSql);

			// STEP:添加数据源字段
			List<DbSourceField> fieldList = table.getFieldList();
			for (DbSourceField field : fieldList) {
				String fieldName = field.getName();
				int isIndex = field.isIndex() ? 1 : 0;
				int isExport = field.isExport() ? 1 : 0;
				addDataSourceField(conn, tableId, fieldName, isIndex, isExport);
			}
		}
	}

	/**
	 * <pre>
	 * 添加数据源表
	 * 
	 * @param conn Connection
	 * @param dataSourceId 数据源Id
	 * @param connectionId 数据源表连接Id
	 * @param tableName 数据源表名
	 * @param tableSql 数据源表Sql
	 * @return 数据源表Id
	 * @throws SQLException 添加数据源表失败时
	 * </pre>
	 */
	private long addDataSourceTable(Connection conn, long dataSourceId,
			long connectionId, String tableName, String tableSql)
			throws SQLException {
		PreparedStatement pstmtForSeq = null;
		PreparedStatement pstmtForAdd = null;
		ResultSet rs = null;
		String sqlForSeq = "select ubp_datasource_table_seq.nextval from dual";
		String sqlForAdd = "insert into ubp_monitor_datasource_table(table_id, datasource_id, connection_id, table_name, table_sql,addtime) values(?, ?, ?, ?, ?,?)";
		long tableId;

		try {
			// STEP1：取sequence
			pstmtForSeq = conn.prepareStatement(sqlForSeq);

			log(logger, conn, sqlForSeq);

			rs = pstmtForSeq.executeQuery();
			rs.next();
			tableId = rs.getLong(1);

			// STEP2：添加数据源表
			pstmtForAdd = conn.prepareStatement(sqlForAdd);
			pstmtForAdd.setLong(1, tableId);
			pstmtForAdd.setLong(2, dataSourceId);
			pstmtForAdd.setLong(3, connectionId);
			pstmtForAdd.setString(4, tableName);
			pstmtForAdd.setString(5, tableSql);
			pstmtForAdd.setTimestamp(6,
					new Timestamp(System.currentTimeMillis()));

			pstmtForAdd.executeUpdate();

			log(logger, conn, sqlForAdd, tableId, dataSourceId, connectionId,
					tableName, tableSql);
		} finally {
			DatabaseUtil.close(rs, pstmtForSeq);
			DatabaseUtil.close(pstmtForAdd);
		}

		return tableId;
	}

	/**
	 * <pre>
	 * 合并数据源字段
	 * 这里有隐患:
	 * 因为本操作默认事务隔离级别为行集锁,本次操作进行时,是在应用程序内存中进行比较,那么其他线程也有可能同时进行插入操作
	 * 而引发多线程问题,从而可能导致同样的字段在数据表内插入两次
	 * @param conn Connection
	 * @param tableId 数据源表Id
	 * @param fieldList 数据源字段列表
	 * @return 当有新字段添加时返回true,否则返回false
	 * @throws SQLException 合并字段失败
	 * @throws 添加失败
	 * </pre>
	 */
	private boolean mergeDataSourceField(Connection conn, long tableId,
			List<DbSourceField> fieldList) throws SQLException {
		// 载入数据源表中所有字段,便于后面快速判断
		Set<String> fieldSet = getDataSourceFieldByTableId(conn, tableId);

		boolean changeFlag = false;

		// 循环所有待添加的字段,如果在数据源表中不存在,则添加
		for (DbSourceField field : fieldList) {
			String fieldName = field.getName();
			if (!fieldSet.contains(fieldName)) {
				int isIndex = field.isIndex() ? 1 : 0;
				int isExport = field.isExport() ? 1 : 0;
				addDataSourceField(conn, tableId, fieldName, isIndex, isExport);
				changeFlag = true;
			}
		}

		return changeFlag;
	}

	/**
	 * <pre>
	 * 获取数据源表中所有字段
	 * 
	 * @param conn Connection
	 * @param tableId 数据源表Id
	 * @return 字段列表
	 * @throws SQLException
	 * </pre>
	 */
	private void delDataSourceFieldByTableId(Connection conn, long tableId)
			throws SQLException {
		PreparedStatement pstmt = null;
		String sql = "delete from ubp_monitor_datasource_field where table_id=?";

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, tableId);

			log(logger, conn, sql, tableId);

			pstmt.executeUpdate();
		} finally {
			DatabaseUtil.close(pstmt);
		}
	}

	/**
	 * <pre>
	 * 获取数据源表中所有字段
	 * 
	 * @param conn Connection
	 * @param tableId 数据源表Id
	 * @return 字段列表
	 * @throws SQLException
	 * </pre>
	 */
	private Set<String> getDataSourceFieldByTableId(Connection conn,
			long tableId) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "select field_name from ubp_monitor_datasource_field where table_id=?";
		Set<String> fieldSet = new HashSet<String>();

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, tableId);

			log(logger, conn, sql, tableId, rs);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				fieldSet.add(rs.getString(1));
			}
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}

		return fieldSet;
	}

	/**
	 * <pre>
	 * 添加数据源字段
	 * 
	 * @param conn Connection
	 * @param tableId 表Id
	 * @param fieldName 字段名
	 * @param isIndex 是否索引字段
	 * @param isExport 是否导出字段
	 * @throws SQLException 添加数据源表字段失败
	 * </pre>
	 */
	private void addDataSourceField(Connection conn, long tableId,
			String fieldName, int isIndex, int isExport) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "insert into ubp_monitor_datasource_field(table_id, field_name, is_index, is_export, order_id) values(?, ?, ?, ?, ubp_datasource_field_seq.nextval)";

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, tableId);
			pstmt.setString(2, fieldName);
			pstmt.setInt(3, isIndex);
			pstmt.setInt(4, isExport);

			log(logger, conn, sql, tableId, fieldName, isIndex, isExport);

			pstmt.executeUpdate();
		} finally {
			DatabaseUtil.close(rs, pstmt);
		}
	}

	/**
	 * <pre>
	 * 删除任务
	 * 
	 * @param monitorTaskId 任务Id
	 * @param callerId 调用者
	 * @return true为删除成功，否则返回false
	 * @throws SQLException 删除失败
	 * </pre>
	 */
	public boolean del(String monitorTaskId, int callerId) throws SQLException {
		Connection conn = null;
		boolean result = false;

		try {
			conn = DbPoolManager
					.getConnection(ApplicationContext.TASK_DATABASE);
			result = del(conn, monitorTaskId, callerId);
		} finally {
			DatabaseUtil.close(conn);
		}

		return result;
	}

	/**
	 * <pre>
	 * 删除任务
	 * 仅删除正常任务，告警清除任务不删除
	 * 
	 * @param monitorTaskId 任务Id
	 * @param callerId 调用者
	 * @return true为删除成功，否则返回false
	 * @throws SQLException 删除失败
	 * </pre>
	 */
	public boolean delWithoutAlarmClear(String monitorTaskId, int callerId)
			throws SQLException {
		Connection conn = null;
		boolean result = false;

		try {
			conn = DbPoolManager
					.getConnection(ApplicationContext.TASK_DATABASE);
			result = delWithoutAlarmClear(conn, monitorTaskId, callerId);
		} finally {
			DatabaseUtil.close(conn);
		}

		return result;
	}

	/**
	 * <pre>
	 * 删除任务
	 * 
	 * @param conn 数据库连接
	 * @param monitorTaskId 任务Id
	 * @param callerId 调用者
	 * @return true为删除成功，否则返回false
	 * @throws SQLException 删除失败
	 * </pre>
	 */
	private boolean del(Connection conn, String monitorTaskId, int callerId)
			throws SQLException {
		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_cfg_task set is_deleted = 1 where is_deleted = 0 and monitor_task_id=? and caller_id=?";
		int affectedRows = 0;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, monitorTaskId);
			pstmt.setInt(2, callerId);

			log(logger, conn, sql, monitorTaskId, callerId);

			affectedRows = pstmt.executeUpdate();
		} finally {
			DatabaseUtil.close(pstmt);
		}

		return affectedRows > 0;
	}

	/**
	 * <pre>
	 * 删除任务
	 * 
	 * @param conn 数据库连接
	 * @param monitorTaskId 任务Id
	 * @param callerId 调用者
	 * @return true为删除成功，否则返回false
	 * @throws SQLException 删除失败
	 * </pre>
	 */
	private boolean delWithoutAlarmClear(Connection conn, String monitorTaskId,
			int callerId) throws SQLException {
		PreparedStatement pstmt = null;
		String sql = "update ubp_monitor_cfg_task set is_deleted = 1 where is_deleted = 0 and is_alarm_clear = 0 and monitor_task_id=? and caller_id=?";
		int affectedRows = 0;

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, monitorTaskId);
			pstmt.setInt(2, callerId);

			log(logger, conn, sql, monitorTaskId, callerId);

			affectedRows = pstmt.executeUpdate();
		} finally {
			DatabaseUtil.close(pstmt);
		}

		return affectedRows > 0;
	}

	/**
	 * <pre>
	 * 修改任务:
	 * 1,删除原任务
	 * 2,添加新任务
	 * 
	 * 2014-07-06，经与任广、杨少江、谢韶光、许瑞林讨论确认
	 * 1、修改任务后，新任务开始运行时间应当是大于等于之前运算过的时间（总的原则：运算过的不再运算，未运算过的需要运算）
	 * 2、统一标准：电信合并数据源时，一个监控任务对应一个数据源。
	 * 		更新修改监控任务时，数据源不变，但要更新SQL、字段（字段全部删除再添加再高效）和数据源时间（数据源时间 = 监控任务运行时间 - 1个分析周期）
	 * 
	 * 2015-04-01，经与邓博文、谢韶光讨论确认：修改任务后，新任务开始运行时间不再与当前数据库中的任务运算时间做比较，直接取传入时间，由外部系统控制时间
	 * 
	 * @param task 新任务信息
	 * @param monitorTaskId 原任务Id
	 * @param callerId 调用者
	 * @throws SQLException
	 * </pre>
	 */
	public void modify(List<MonitorTask> taskList, String monitorTaskId,
			int callerId) throws SQLException {
		Connection conn = null;

		try {
			conn = DbPoolManager
					.getConnection(ApplicationContext.TASK_DATABASE);
			conn.setAutoCommit(false);

			// 算出原来任务的最大CurrMonitorTime
			if (Configuration.getBoolean(Configuration.CALIBRATION_CURR_MONITOR_TIME)) {
				for (MonitorTask task : taskList) {
					// “告警task”和“结单task”需要分别继承原任务的CurrMonitorTime
					Timestamp maxCurrMonitorTime = getMaxCurrMonitorTime(monitorTaskId,task.getAlarmClear());
					if (maxCurrMonitorTime.after(task.getCurrMonitorTime()))
						task.setCurrMonitorTime(maxCurrMonitorTime);
				}
			}

			boolean b = del(conn, monitorTaskId, callerId);
			if (!b)
				throw new SQLException("删除失败，监控任务" + monitorTaskId + "不存在");

			add(conn, taskList);

			conn.commit();
		} catch (SQLException e) {
			if (conn != null)
				conn.rollback();

			throw e;
		} finally {
			DatabaseUtil.close(conn);
		}
	}

	/**
	 * 根据监控任务Id获取监控任务中所有任务最大的监控时间
	 * 
	 * @param monitorTaskId
	 * @param is_alarm_clear
	 * @return
	 * @throws SQLException
	 */
	private Timestamp getMaxCurrMonitorTime(String monitorTaskId, int is_alarm_clear)
			throws SQLException {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Timestamp maxCurrMonitorTime = null;

		String sql = "select max(t.curr_monitor_time) from ubp_monitor_cfg_task t where t.is_deleted=0 and t.monitor_task_id=? and is_alarm_clear=?";

		try {
			conn = DbPoolManager.getConnectionForTask();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, monitorTaskId);
			pstmt.setInt(2, is_alarm_clear);

			log(logger, conn, sql, monitorTaskId, is_alarm_clear);

			rs = pstmt.executeQuery();

			if (rs.next()) {
				maxCurrMonitorTime = rs.getTimestamp(1);
			}
		} finally {
			DatabaseUtil.close(conn, pstmt, rs);
		}

		return maxCurrMonitorTime;
	}

	/**
	 * <pre>
	 * 设置监控任务启用状态
	 * 
	 * @param monitorTaskId 监控任务Id
	 * @param callerId 调用者Id
	 * @param oper 操作方式 1全部启用，0全部禁用，-1仅启用告警清除
	 * @return 设置成功:受修改操作影响的记录行数>0(不考虑>1的情况)的情况true；对于仅启用告警清除的情况，始终返回true
	 * @throws SQLException 设置失败
	 * </pre>
	 */
	public boolean setUsedStatus(String monitorTaskId, int callerId, int oper)
			throws SQLException {
		Connection conn = null;
		PreparedStatement pstmt1 = null;
		PreparedStatement pstmt2 = null;
		String sql = "update ubp_monitor_cfg_task set is_used=? where monitor_task_id=? and caller_id=?";
		int affectedRows = 0;

		try {
			conn = DbPoolManager
					.getConnection(ApplicationContext.TASK_DATABASE);
			switch (oper) {
				case 1 : // 全部启用
					pstmt1 = conn.prepareStatement(sql);
					pstmt1.setInt(1, 1);
					pstmt1.setString(2, monitorTaskId);
					pstmt1.setInt(3, callerId);

					log(logger, conn, sql, 1, monitorTaskId, callerId);

					affectedRows = pstmt1.executeUpdate();

					break;
				case 0 : // 全部禁用
					pstmt1 = conn.prepareStatement(sql);
					pstmt1.setInt(1, 0);
					pstmt1.setString(2, monitorTaskId);
					pstmt1.setInt(3, callerId);

					log(logger, conn, sql, 0, monitorTaskId, callerId);

					affectedRows = pstmt1.executeUpdate();

					break;
				case -1 : // 仅启用告警清除
					// STEP1：禁用正常任务
					sql += " and is_alarm_clear=?";

					pstmt1 = conn.prepareStatement(sql);
					pstmt1.setInt(1, 0);
					pstmt1.setString(2, monitorTaskId);
					pstmt1.setInt(3, callerId);
					pstmt1.setInt(4, 0);

					log(logger, conn, sql, 0, monitorTaskId, callerId, 0);

					pstmt1.executeUpdate();

					// STEP2：启用告警清除任务
					pstmt2 = conn.prepareStatement(sql);
					pstmt2.setInt(1, 1);
					pstmt2.setString(2, monitorTaskId);
					pstmt2.setInt(3, callerId);
					pstmt2.setInt(4, 1);

					log(logger, conn, sql, 1, monitorTaskId, callerId, 1);

					pstmt2.executeUpdate();

					affectedRows = 1;

					break;
				case 2 :
					// 将驳回任务设置为启用
					sql += " and is_alarm_clear=2";
					pstmt1 = conn.prepareStatement(sql);
					pstmt1.setInt(1, 1);
					pstmt1.setString(2, monitorTaskId);
					pstmt1.setInt(3, callerId);
					affectedRows = pstmt1.executeUpdate();

					log(logger, conn, sql, 1, monitorTaskId, callerId);

					break;
			}
		} finally {
			DatabaseUtil.close(pstmt1, pstmt2);
			DatabaseUtil.close(conn);
		}

		return affectedRows > 0;
	}

	/**
	 * <pre>
	 * 查找给定数据源对应的所有监控任务
	 * 
	 * @param monitorTaskId 数据源Id
	 * @return 给定数据源对应的所有监控任务
	 * @throws SQLException
	 * </pre>
	 */
	public List<MonitorTask> getMonitorTasks(String monitorTaskId)
			throws SQLException {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		// 查全表数据
		String sql = "SELECT T.TASK_ID,T.TASK_NAME,T.TASK_DESCRIPTION,T.MONITOR_TASK_ID,T.CALLER_ID,T.IS_USED,T.IS_DELETED"
				+ ",T.IS_PERIOD,T.PERIOD_NUM,T.PERIOD_UNIT,T.DATASOURCE_ID,T.CURR_MONITOR_TIME,T.END_MONITOR_TIME,T.GROUP_ID,T.PC_NAME,T.CITY_ID"
				+ ",T.IS_VALID,T.PI_NAME,T.PI_EXPR_DESCRIPTION,T.FILTER,T.KEY_INDEX_TYPE,T.T_ID,T.IS_ALARM_CLEAR,T.MONITOR_FIELD,T.EXPRESSION_INFO"
				+ ",T.PERIOD_INFO,T.TOP_INFO,T.ALARM_LEVEL_INFO,T.RULE_DESCRIPTION,T.MEMO FROM UBP_MONITOR_CFG_TASK T"
				+ " WHERE T.IS_DELETED=0 AND T.MONITOR_TASK_ID=?";

		List<MonitorTask> monitorTaskList;

		try {
			conn = DbPoolManager.getConnectionForTask();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, monitorTaskId);

			log(logger, conn, sql, monitorTaskId);

			rs = pstmt.executeQuery();

			monitorTaskList = new ArrayList<>();

			while (rs.next()) {
				MonitorTask task;
				try {
					task = new MonitorTask(rs.getLong(1), rs.getString(2),
							rs.getString(3), rs.getString(4), rs.getInt(5),
							rs.getInt(6), rs.getInt(7), rs.getInt(8),
							rs.getInt(9), rs.getString(10), rs.getLong(11),
							rs.getTimestamp(12), rs.getTimestamp(13),
							rs.getInt(14), rs.getString(15), rs.getInt(16),
							rs.getInt(17), rs.getString(18), rs.getString(19),
							rs.getString(20), rs.getInt(21), rs.getInt(22),
							rs.getInt(23), rs.getString(24), rs.getString(25),
							rs.getString(26), rs.getString(27),
							rs.getString(28), rs.getString(29),
							rs.getString(30));
					monitorTaskList.add(task);
				} catch (Exception e) {
					logger.error("解析监控任务失败", e);

					continue;
				}
			}
		} finally {
			DatabaseUtil.close(conn, pstmt, rs);
		}

		return monitorTaskList;
	}

	/**
	 * <pre>
	 * 检查任务是否合法
	 * 
	 * @param monitorTaskId 任务Id
	 * @param callerId 调用者Id
	 * @return 任务Id是否有效
	 * </pre>
	 */
	/*
	 * @Deprecated public Integer getMonitorTaskValidStatus(String
	 * monitorTaskId, int callerId) { SqlSession session =
	 * SqlSessionManager.getSqlSessionFactory().openSession();
	 * 
	 * try { MonitorTaskMapper mtMapper =
	 * session.getMapper(MonitorTaskMapper.class); Integer isValid =
	 * mtMapper.getMonitorTaskValid(monitorTaskId, callerId);
	 * 
	 * session.commit();
	 * 
	 * return isValid; } finally { session.close(); } }
	 */

}
