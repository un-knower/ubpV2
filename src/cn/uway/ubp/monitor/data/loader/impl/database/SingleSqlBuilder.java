package cn.uway.ubp.monitor.data.loader.impl.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import cn.uway.framework.data.model.FieldType;
import cn.uway.framework.util.DateTimeUtil;
import cn.uway.ubp.monitor.dao.DataSourceDAO;
import cn.uway.ubp.monitor.dao.ViewInfoDAO;
import cn.uway.util.DateGranularityUtil;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.DbSourceInfo;
import cn.uway.util.entity.DbSourceTable;
import cn.uway.util.entity.ExField;
import cn.uway.util.entity.ViewField;
import cn.uway.util.entity.ViewInfo;

/**
 * <pre>
 * 单库SQL创建器
 * 根据监控数据源配置信息生成SQL语句
 * @author chris
 * @ 2014年3月12日
 * </pre>
 */
public class SingleSqlBuilder {

	/**
	 * 数据源信息
	 */
	private DataSource dataSource;

	/**
	 * 数据源表ID和名称的对应关系 K=数据源表ID，V=数据源表名称
	 */
	private Map<Long, String> tableMapping;

	/**
	 * 数据源对应的字段信息
	 */
	private List<ExField> fieldInfos;

	/**
	 * 对于电信使用SQL数据源表的情况，支持时间过滤标签，提升查询性能
	 */
	private final String TIME_FLAG_UPPER = "#{TIME}";

	private final String TIME_FLAG_LOWER = "#{time}";

	public SingleSqlBuilder(Connection conn, DataSource dataSource) throws SQLException {
		if (dataSource == null)
			throw new NullPointerException("组装SQL失败,数据源信息为空");

		this.dataSource = dataSource;

		initialize(conn);
	}

	/**
	 * 初始化数据源对应的表和字段信息
	 * 
	 * @throws SQLException
	 */
	private void initialize(Connection conn) throws SQLException {
		List<DbSourceTable> tableList = dataSource.getDbSourceInfo()
				.getDbSourceTableList();
		if (tableList == null || tableList.isEmpty())
			throw new NullPointerException("组装SQL失败,数据源配置中表信息为空");

		fieldInfos = DataSourceDAO.getInstance().getFieldListWihView(
				conn, dataSource.getId());
		if (fieldInfos == null || fieldInfos.size() == 0)
			throw new NullPointerException("组装SQL失败,数据源配置中字段信息为空");

		// 初始化表关系（表ID和名称）映射
		tableMapping = new HashMap<>();
		for (DbSourceTable table : tableList) {
			tableMapping.put(table.getTableId(), table.getName());
		}
	}

	/**
	 * <pre>
	 * 创建SQL
	 * 
	 * @return sql语句
	 * @throws SQLException
	 * </pre>
	 */
	public String build(Connection taskConn) throws SQLException {
		StringBuilder sql = new StringBuilder();

		DbSourceInfo dbSourceInfo = dataSource.getDbSourceInfo();
		// 时间字段
		String timeField = dbSourceInfo.getTimeFieldName();
		String timeFieldType = dbSourceInfo.getTimeFieldType();
		// 拼接字段
		ExField field = fieldInfos.remove(0);

		// 在新的topN的算法中timeField 和 timeFieldType 这两个字段为空
		if (StringUtils.isNotBlank(timeField)) {
			sql.append("select ");
			sql.append(tableMapping.get(field.getTableId())).append(".")
					.append(field.getName());
			Iterator<ExField> iterator = fieldInfos.iterator();
			while (iterator.hasNext()) {
				field = iterator.next();
				sql.append(",").append(tableMapping.get(field.getTableId()))
						.append(".").append(field.getName());
			}
		} else {
			// 在传入的sql外层添加start_time 字段
			sql.append(" select * from ( select ");

			sql.append(tableMapping.get(field.getTableId()))
					.append(".*")
					.append(",")
					.append(" to_date( '")
					.append(DateTimeUtil.formatDateTime(dataSource
							.getDataTime())
							+ "', 'yyyy-mm-dd hh24:mi:ss')  as START_TIME");

		}

		// 拼接表
		List<DbSourceTable> tableList = dbSourceInfo.getDbSourceTableList();
		sql.append(" from ");
		for (DbSourceTable table : tableList) {
			String tableStr = table.toString();
			// 对于电信使用SQL数据源表的情况，支持时间过滤标签，提升查询性能
			// 支持 time_flag 标签大小写
			if (StringUtils.isNotBlank(table.getSql())) {
				if (tableStr.contains(TIME_FLAG_UPPER)) {
					tableStr = tableStr.replaceAll(
							"#\\{TIME\\}",
							"to_date('"
									+ DateTimeUtil.formatDateTime(dataSource
											.getDataTime())
									+ "', 'yyyy-mm-dd hh24:mi:ss') ");
				}
				if (tableStr.contains(TIME_FLAG_LOWER)) {
					tableStr = tableStr.replaceAll(
							"#\\{time\\}",
							"to_date('"
									+ DateTimeUtil.formatDateTime(dataSource
											.getDataTime())
									+ "', 'yyyy-mm-dd hh24:mi:ss') ");

				}
			}

			sql.append(tableStr).append(",");
		}

		if (',' == sql.charAt(sql.length() - 1)) {
			sql.deleteCharAt(sql.length() - 1);
		}

		if (StringUtils.isBlank(timeField)) {
			sql.append(" ) ").append(tableMapping.get(field.getTableId()));
			timeField = "START_TIME";
			timeFieldType = "DATE";
		}
		
		// 拼接条件
		sql.append(" where 1=1 and ");
		String tableRelation = dbSourceInfo.getTableRelation();
		if (StringUtils.isNotBlank(tableRelation)) {
			tableRelation = tableRelation.toLowerCase();
			List<ViewField> fieldNameList = DataSourceDAO.getInstance()
					.getViewFieldByDateSourceId(taskConn, dataSource.getId());
			// 字段替换
			for (ViewField viewField : fieldNameList)
				tableRelation = tableRelation.replace(viewField.getAsName(),
						viewField.getSourceName());
			// 表名替换
			List<ViewInfo> viewInfoList = ViewInfoDAO.getInstance()
					.getViewInfosByDataSourceId(taskConn, dataSource.getId());
			for (ViewInfo viewInfo : viewInfoList) {
				tableRelation = tableRelation.replace(viewInfo.getViewName(),
						viewInfo.getViewMainTalbe());
			}
			sql.append(tableRelation).append(" and ");
		}
		
		// 视图对应关联的实体表的关联条件
		List<DataSource> dataSourceList = DataSourceDAO.getInstance()
				.getDataSourceWithViewInfo(taskConn,dataSource.getId());
		if (dataSourceList != null && !dataSourceList.isEmpty()) {
			for (DataSource dataSource : dataSourceList) {
				sql.append(dataSource.getDbSourceInfo().getTableRelation())
				.append(" and ");
			}
		}

		if (StringUtils.isNotBlank(timeFieldType)) {
			timeFieldType = timeFieldType.toLowerCase();
			switch (timeFieldType) {
				case FieldType.STRING :
					// TODO: String类型的不能作时间范围区间比较，此处暂时用"="操作符比较
					sql.append(dbSourceInfo.getTimeFieldTable()).append(".")
							.append(timeField).append(" = ");
					sql.append("'")
							.append(DateTimeUtil.formatDateTime(dataSource
									.getDataTime())).append("'");
					break;
				case FieldType.DATE : {
					// 时间作区间比较
					Date startTime = dataSource.getDataTime();
					Date endTime = DateGranularityUtil.forwardTimeTravel(
							dataSource.getDataTime(), dataSource
									.getGranularity().toString(), 1);

					sql.append("(");
					sql.append(dbSourceInfo.getTimeFieldTable()).append(".")
							.append(timeField).append(" >= ");
					sql.append("to_date('")
							.append(DateTimeUtil.formatDateTime(startTime))
							.append("', 'yyyy-mm-dd hh24:mi:ss')");
					sql.append(" and ");
					sql.append(dbSourceInfo.getTimeFieldTable()).append(".")
							.append(timeField).append(" < ");
					sql.append("to_date('")
							.append(DateTimeUtil.formatDateTime(endTime))
							.append("', 'yyyy-mm-dd hh24:mi:ss')");
					sql.append(")");

					break;
				}
				default :
					throw new IllegalArgumentException(
							"数据时间字段类型只能为String或Date类型");
			}

		}

		/**
		 * 数据完整性保护,延时delay个单位分钟 仅对SQL方式数据源有效
		 */
		DbSourceTable dbTable = dbSourceInfo.getDbSourceTableList().get(0);
		if (StringUtils.isNotBlank(timeFieldType)) {
			timeFieldType = timeFieldType.toLowerCase();
			if (StringUtils.isNotBlank(dbTable.getSql())) {
				sql.append(" and ");
				int dataDelay = getDataDelay();
				switch (timeFieldType) {
					case FieldType.STRING :
						sql.append("to_date('").append(timeField)
								.append("','yyyy-mm-dd hh24:mi:ss'");
						break;
					case FieldType.DATE :
						sql.append(timeField);
						break;
					default :
						throw new IllegalArgumentException(
								"数据时间字段类型只能为String或Date类型");
				}
				sql.append(" < (sysdate-").append(dataDelay).append("/60/24)");
			}
		}
		return sql.toString();
	}

	/**
	 * <pre>
	 * 获取数据延时时间
	 * 
	 * 默认取数据源表中延时字段值
	 * 如果延时字段值为0，则按默认标准返回数据延时时间
	 * 默认标准：
	 * HOUR：6*60（6小时）
	 * DAY：2*24*60（2天）
	 * WEEK：(7 + 3) * 24 * 60（1周零7天）
	 * MONTH：(31 + 7) * 24 * 60（1个月零7天）
	 * SEASON：(31 * 3 + 7) * 24 * 60（1个季度零7天）
	 * YEAR：(365 + 7) * 24 * 60（1年零7天）
	 * 默认（MINUTE）：24 * 60（1天）
	 * @return 数据延时，单位分钟
	 * </pre>
	 */
	private int getDataDelay() {
		int dataDelay = dataSource.getDbSourceInfo().getDataDelay();
		if (dataDelay == 0) {
			switch (dataSource.getGranularity()) {
				case HOUR :
					dataDelay = 6 * 60;
					break;
				case DAY :
					dataDelay = 2 * 24 * 60;
					break;
				case WEEK :
					dataDelay = (7 + 3) * 24 * 60;
					break;
				case MONTH :
					dataDelay = (31 + 7) * 24 * 60;
					break;
				case SEASON :
					dataDelay = (31 * 3 + 7) * 24 * 60;
					break;
				case YEAR :
					dataDelay = (365 + 7) * 24 * 60;
					break;
				default :
					dataDelay = 24 * 60; // 默认值应当不妥
			}
		}

		return dataDelay;
	}

}
