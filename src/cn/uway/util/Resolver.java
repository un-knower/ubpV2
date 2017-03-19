package cn.uway.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.xml.sax.SAXException;

import cn.uway.framework.el.extend.FormulaExpression;
import cn.uway.framework.util.TimeUtil;
import cn.uway.util.entity.AlarmLevel;
import cn.uway.util.entity.AlarmLevel.Alarm;
import cn.uway.util.entity.AnalysisPeriod;
import cn.uway.util.entity.AssignTime;
import cn.uway.util.entity.DataRange;
import cn.uway.util.entity.DataSource;
import cn.uway.util.entity.DbSourceField;
import cn.uway.util.entity.DbSourceInfo;
import cn.uway.util.entity.DbSourceTable;
import cn.uway.util.entity.Exclude;
import cn.uway.util.entity.ExcludeSql;
import cn.uway.util.entity.Expression;
import cn.uway.util.entity.Filter;
import cn.uway.util.entity.Holiday;
import cn.uway.util.entity.Include;
import cn.uway.util.entity.IncludeSql;
import cn.uway.util.entity.Indicator;
import cn.uway.util.entity.MonitorPeriod;
import cn.uway.util.entity.MonitorTask;
import cn.uway.util.entity.OccurTimes;
import cn.uway.util.entity.Offset;
import cn.uway.util.entity.OrderIndicator;
import cn.uway.util.entity.PeriodInfo;
import cn.uway.util.entity.Range;
import cn.uway.util.entity.Rule;
import cn.uway.util.entity.SecureInfo;
import cn.uway.util.entity.SubOccurTimes;
import cn.uway.util.entity.Top;
import cn.uway.util.enums.SortWay;
import cn.uway.util.enums.TimeUnit;

/**
 * 解析请求的报文信息
 * 
 * @author Chris 2013年10月28日
 */
public class Resolver {

	/**
	 * 安全校验XSD文件
	 */
	private static final URL URL_UPORT_SECURE_VALIDATE = Thread.currentThread()
			.getContextClassLoader()
			.getResource("cn/uway/util/resource/xsd/uport-secure-validate.xsd");

	/**
	 * 监控任务下发XSD文件
	 */
	private static final URL URL_UBP_MONITOR_TASK = Thread.currentThread()
			.getContextClassLoader()
			.getResource("cn/uway/util/resource/xsd/ubp-monitor-task.xsd");

	/**
	 * 监控任务Id最大长度
	 */
	private static final int MAX_LENGTH_MONITOR_TASK_ID = 128;

	/**
	 * 监控任务名称最大长度
	 */
	private static final int MAX_LENGTH_MONITOR_TASK_NAME = 128;

	/**
	 * 主指标名最大长度
	 */
	private static final int MAX_LENGTH_PRIMARY_INDICATOR_NAME = 500;

	/**
	 * 主指标描述最大长度
	 */
	private static final int MAX_LENGTH_PRIMARY_INDICATOR_EXPR_DESCRIPTION = 500;

	/**
	 * 表名、视图名最大长度
	 */
	private static final int MAX_LENGTH_TABLE_NAME = 128;

	/**
	 * 字段名最大长度
	 */
	private static final int MAX_LENGTH_FIELD_NAME = 32;

	/**
	 * 数据源级别最大长度
	 */
	private static final int MAX_LENGTH_NE_LEVEL = 20;

	/**
	 * 网络类型最大长度
	 */
	private static final int MAX_LENGTH_NET_TYPE = 20;

	/**
	 * 多表关联时主表名最大长度
	 */
	private static final int MAX_LENGTH_MASTER_TABLE_NAME = 50;

	/**
	 * 多表关联时主表的时间字段
	 */
	private static final int MAX_LENGTH_TIME_FIELD = 50;

	/**
	 * 多表关联时主表的时间字段类型
	 */
	private static final int MAX_LENGTH_TIME_FIELD_TYPE = 20;

	/**
	 * 规则信息描述的最大长度
	 */
	private static final int MAX_LENGTH_RULE_DESCRIPTION = 2000;

	/**
	 * 分析规则描述的最大长度
	 */
	private static final int MAX_LENGTH_RULE_PERIOD_INFO = 2000;

	/**
	 * TOPN规则描述的最大长度
	 */
	private static final int MAX_LENGTH_RULE_TOP_INFO = 2000;

	/**
	 * 告警级别定义的最大长度
	 */
	private static final int MAX_LENGTH_RULE_ALARM_LEVEL = 2000;

	/**
	 * 对请求的字符串编码
	 */
	private static final Charset CHARSET = Charset.forName("utf-8");

	/**
	 * 日期时间格式化串 XSD日期时间格式
	 */
	private static final String DEFAULT_DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";

	/**
	 * 数据源类型：数据库
	 */
	private static final String DATA_SOURCE_TYPE_DATABASE = "database";

	/**
	 * 数据源类型：文件
	 */
	private static final String DATA_SOURCE_TYPE_FILE = "file";

	/**
	 * Memo分隔字符
	 */
	private static final char US = '\31';

	/**
	 * <pre>
	 * 解析安全信息请求
	 * 
	 * @param xml 请求数据
	 * @return 安全对象实体类
	 * @throws Exception 解析XML失败
	 * </pre>
	 */
	public static SecureInfo resolveSecureInfo(String xml, int calledId)
			throws Exception {
		// 如果安全校验信息xml为空，直接抛出空指针异常
		if (StringUtils.isBlank(xml))
			throw new NullPointerException("安全校验xml为空");

		// validation by xsd
		InputStream is = null;
		try {
			byte[] buf = xml.getBytes(CHARSET);
			is = new ByteArrayInputStream(buf);
			validationSecure(is);
		} finally {
			if (is != null) {
				is.close();
			}
		}

		Document doc = DocumentHelper.parseText(xml);
		Element root = doc.getRootElement();
		String callerIdStr = root.elementText("caller-id");
		int callerId = Integer.parseInt(callerIdStr);
		String username = root.elementText("username");
		String password = root.elementText("password");

		return new SecureInfo(callerId, calledId, username, password);
	}

	/**
	 * <pre>
	 * 解析添加任务的请求
	 * 
	 * @param taskXml 请求数据
	 * @return 添加任务请求的实体类
	 * @throws Exception
	 * </pre>
	 */
	public static List<MonitorTask> resolveMonitorTask(String taskXml)
			throws Exception {
		// 如果任务信息xml为空，直接抛出空指针异常
		if (StringUtils.isBlank(taskXml))
			throw new NullPointerException("任务xml为空");

		// validation by xsd
		InputStream is = null;
		try {
			byte[] buf = taskXml.getBytes(CHARSET);
			is = new ByteArrayInputStream(buf);
			validationTask(is);
		} finally {
			if (is != null) {
				is.close();
			}
		}

		Document doc = DocumentHelper.parseText(taskXml);
		Element root = doc.getRootElement();

		// task base info
		MonitorTask task = resolveTaskBaseInfo(root);

		// datasource
		Element dataSourceElement = root.element("datasource");
		DataSource dataSource = resolveDataSource(dataSourceElement);
		task.setDataSource(dataSource);

		// jobs
		Set<String> fieldSet = dataSource.getFieldSet();
		Element jobsElement = root.element("jobs");
		List<MonitorTask> taskList = resolveMonitorTaskJobs(jobsElement,
				fieldSet, task);
		return taskList;
	}

	/**
	 * <pre>
	 * 解析任务基本信息
	 * 
	 * @param root 根节点
	 * @return 任务基础信息
	 * @throws IllegalArgumentException 非法参数
	 * </pre>
	 */
	private static MonitorTask resolveTaskBaseInfo(Element root) {
		MonitorTask task = new MonitorTask();

		// 解析监控任务ID
		String monitorTaskId = root.attributeValue("id").trim();
		int monitorTaskIdLength = monitorTaskId.getBytes(CHARSET).length;
		if (monitorTaskIdLength > MAX_LENGTH_MONITOR_TASK_ID)
			throw new IllegalArgumentException("错误的任务定义,根节点id属性超出最大长度"
					+ MAX_LENGTH_MONITOR_TASK_ID + "，实际长度为"
					+ monitorTaskIdLength);
		task.setMonitorTaskId(monitorTaskId);

		// 在业务系统的监控任务名称
		String monitorTaskName = root.attributeValue("name").trim();
		int monitorTaskNameLength = monitorTaskName.getBytes(CHARSET).length;
		if (monitorTaskNameLength > MAX_LENGTH_MONITOR_TASK_NAME)
			throw new IllegalArgumentException("错误的任务定义,根节点name属性超出最大长度"
					+ MAX_LENGTH_MONITOR_TASK_NAME + "，实际长度为"
					+ monitorTaskNameLength);
		task.setTaskName(monitorTaskName);

		// 是否立即启用监控任务
		String isUsedStr = root.attributeValue("enable");
		if (StringUtils.isNotBlank(isUsedStr)) {
			Boolean isUsed = Boolean.parseBoolean(isUsedStr);
			task.setUsed(isUsed);
		}

		// 调用系统ID
		String callerStr = root.elementText("caller");
		int caller = Integer.parseInt(callerStr);
		task.setCallerId(caller);

		// t-id 电信用
		String tIdStr = root.elementText("t-id");
		if (StringUtils.isNotBlank(tIdStr)) {
			int tId = Integer.parseInt(tIdStr);
			task.settId(tId);
		}

		// key-index-type 电信用
		String keyIndexTypeStr = root.elementText("key-index-type");
		if (StringUtils.isNotBlank(keyIndexTypeStr)) {
			int keyIndexType = Integer.parseInt(keyIndexTypeStr);
			task.setKeyIndexType(keyIndexType);
		}

		// first-monitor-time 第一次监控时间
		String firstMonitorTimeStr = root.elementText("first-monitor-time");
		Timestamp firstMonitorTime;
		try {
			Date date = TimeUtil.getDate(firstMonitorTimeStr,
					DEFAULT_DATE_TIME_FORMAT);
			firstMonitorTime = new Timestamp(date.getTime());
			task.setCurrMonitorTime(firstMonitorTime);
		} catch (Exception e) {
			throw new IllegalArgumentException(
					"错误的任务定义,<first-monitor-time>节点值不是有效日期:yyyy-MM-ddTHH:mm:ss",
					e);
		}

		// last-monitor-time 监控截止时间，可选节点
		String lastMonitorTimeStr = root.elementText("last-monitor-time");
		if (StringUtils.isNotBlank(lastMonitorTimeStr)) {
			Timestamp lastMonitorTime;
			try {
				Date date = TimeUtil.getDate(lastMonitorTimeStr,
						DEFAULT_DATE_TIME_FORMAT);
				lastMonitorTime = new Timestamp(date.getTime());
			} catch (Exception e) {
				throw new IllegalArgumentException(
						"错误的任务定义,<last-monitor-time>节点值不是有效日期:yyyy-MM-ddTHH:mm:ss",
						e);
			}
			if (firstMonitorTime.compareTo(lastMonitorTime) >= 0)
				throw new IllegalArgumentException(
						"错误的任务定义,<first-monitor-time>节点值必须早于<last-monitor-time>节点值");
			task.setEndMonitorTime(lastMonitorTime);
		}

		// city
		String cityStr = root.elementTextTrim("city");
		int city = Integer.parseInt(cityStr);
		task.setCityId(city);

		// monitor-period-time
		// 2013-09-12 增加时间单位 避免程序中进行滑动窗口，固定窗口判断
		Element periodElement = root.element("monitor-period-time");
		String periodTimeUnit = periodElement.attributeValue("unit");
		String monitorPeriodTimeNumberStr = periodElement
				.attributeValue("number");
		int periodTimeNumber = Integer.parseInt(monitorPeriodTimeNumberStr);
		task.setPeriodUnit(TimeUnit.valueOf(periodTimeUnit));
		task.setPeriodNum(periodTimeNumber);

		// memo
		Element memoElement = root.element("memo");
		String[] memoAry = new String[10];
		if (memoElement != null) {
			List<?> elementList = memoElement.elements();
			for (Object obj : elementList) {
				try {
					Element element = (Element) obj;
					String name = element.getName();
					String idxStr = StringUtils.substring(name, 1);
					if (!StringUtils.isNumeric(idxStr))
						continue;

					int idx = Integer.parseInt(idxStr);
					memoAry[idx] = element.getText();
				} catch (Exception e) {
					// 跳过非C0到C9的节点
				}
			}
		}
		String memo = StringUtils.join(memoAry, US);
		task.setMemo(memo);

		// primary-indicator 主指标定义，联通用，可选节点
		Element primaryIndicatorElement = root.element("primary-indicator");
		if (primaryIndicatorElement != null) {
			// primary-indicator -> name
			Element nameElement = primaryIndicatorElement.element("name");
			String primaryIndicatorName = nameElement.getTextTrim();
			int primaryIndicatorNameLength = primaryIndicatorName
					.getBytes(CHARSET).length;
			if (primaryIndicatorNameLength > MAX_LENGTH_PRIMARY_INDICATOR_NAME)
				throw new IllegalArgumentException(
						"错误的任务定义,<primary-indicator>节点中<name>节点超出最大长度"
								+ MAX_LENGTH_PRIMARY_INDICATOR_NAME + "，实际长度为"
								+ primaryIndicatorNameLength);
			task.setPiName(primaryIndicatorName);

			// primary-indicator -> expr-description
			Element exprDescriptionElement = primaryIndicatorElement
					.element("expr-description");
			String primaryIndicatorExprDescription = exprDescriptionElement
					.getTextTrim();
			int primaryIndicatorExprDescriptionLength = primaryIndicatorExprDescription
					.getBytes(CHARSET).length;
			if (primaryIndicatorExprDescriptionLength > MAX_LENGTH_PRIMARY_INDICATOR_EXPR_DESCRIPTION)
				throw new IllegalArgumentException(
						"错误的任务定义,<primary-indicator>节点中<expr-description>节点超出最大长度"
								+ MAX_LENGTH_PRIMARY_INDICATOR_EXPR_DESCRIPTION
								+ "，实际长度为"
								+ primaryIndicatorExprDescriptionLength);
			task.setPiExprDescription(primaryIndicatorExprDescription);
		}

		return task;
	}

	/**
	 * <pre>
	 * 解析数据源
	 * 
	 * @param element 数据源节点
	 * </pre>
	 */
	private static DataSource resolveDataSource(Element element) {
		DataSource dataSource = new DataSource();

		// gran
		String granularity = element.attributeValue("gran");
		dataSource.setGranularity(TimeUnit.valueOf(granularity));

		// level
		String neLevel = element.attributeValue("level");
		int neLevelLength = neLevel.getBytes(CHARSET).length;
		if (neLevelLength > MAX_LENGTH_NE_LEVEL)
			throw new IllegalArgumentException(
					"错误的数据源定义,<datasource>节点中level属性超出最大长度"
							+ MAX_LENGTH_NE_LEVEL + "，实际长度为" + neLevelLength);
		dataSource.setNeLevel(neLevel);

		// net type
		String netType = element.attributeValue("net-type");
		int netTypeLength = netType.getBytes(CHARSET).length;
		if (netTypeLength > MAX_LENGTH_NET_TYPE)
			throw new IllegalArgumentException(
					"错误的数据源定义,<datasource>节点中net-type属性超出最大长度"
							+ MAX_LENGTH_NET_TYPE + "，实际长度为" + netTypeLength);
		dataSource.setNetType(netType);

		String isLogDrive = element.attributeValue("is-log-drive");
		dataSource.setLogDrive(Boolean.valueOf(isLogDrive));

		// 数据源表字段集合
		Set<String> fieldSet = new HashSet<String>();
		List<?> elementList = element.elements();
		if (elementList == null || elementList.isEmpty())
			throw new IllegalArgumentException(
					"错误的数据源定义,<datasource>节点中未定义具体的数据源类型");

		for (Object obj : elementList) {
			if (obj instanceof Element) {
				Element subElement = (Element) obj;
				String elementName = subElement.getName();
				switch (elementName) {
				// 数据库类型数据源描述
					case DATA_SOURCE_TYPE_DATABASE :
						// 校验
						DbSourceInfo dbSourceInfo = resolveDbSource(subElement);
						dataSource.setDbSourceInfo(dbSourceInfo);

						// 将字段添加至集合中
						List<DbSourceTable> tableList = dbSourceInfo
								.getDbSourceTableList();
						for (DbSourceTable table : tableList) {
							List<DbSourceField> fieldList = table
									.getFieldList();
							for (DbSourceField field : fieldList) {
								String fieldName = field.getName();
								fieldSet.add(fieldName);
							}
						}

						dataSource.setType(0);

						break;
					// 文件类型数据源描述
					case DATA_SOURCE_TYPE_FILE :
						// 校验
						DbSourceInfo _dbSourceInfo = resolveFileDataSource(subElement);
						dataSource.setDbSourceInfo(_dbSourceInfo);

						// 将字段添加至集合中
						List<DbSourceTable> _tableList = _dbSourceInfo
								.getDbSourceTableList();
						for (DbSourceTable table : _tableList) {
							List<DbSourceField> fieldList = table
									.getFieldList();
							for (DbSourceField field : fieldList) {
								String fieldName = field.getName();
								fieldSet.add(fieldName);
							}
						}

						dataSource.setType(1);

						break;
					default :
						// other data source OR to log?
				}
			}
		}

		dataSource.setFieldSet(fieldSet);

		return dataSource;
	}

	/**
	 * <pre>
	 * 解析DB数据源
	 * 
	 * @param element 数据源节点
	 * @return DB数据源信息
	 * @throws IllegalArgumentException 非法参数
	 * </pre>
	 */
	private static DbSourceInfo resolveDbSource(Element element) {
		DbSourceInfo dbSourceInfo = new DbSourceInfo();

		// data-delay
		Element dataDelayElement = element.element("data-delay");
		String dataDelayStr = dataDelayElement.getText();
		int dataDelay;
		try {
			dataDelay = Integer.parseInt(dataDelayStr);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(
					"错误的数据源定义,<database>节点中<data-delay>子节点值不是有效整型", e);
		}
		dbSourceInfo.setDataDelay(dataDelay);

		// time-field
		Element timeFieldElement = element.element("time-field");

		String timeFieldName = timeFieldElement.attributeValue("name");
		int timeFieldNameLength = timeFieldName.getBytes(CHARSET).length;
		if (timeFieldNameLength > MAX_LENGTH_TIME_FIELD)
			throw new IllegalArgumentException(
					"错误的任务定义,<database>节点中<time-field>节点name属性超出最大长度"
							+ MAX_LENGTH_TIME_FIELD + "，实际长度为"
							+ timeFieldNameLength);
		dbSourceInfo.setTimeFieldName(timeFieldName);

		String timeFieldType = timeFieldElement.attributeValue("type");
		int timeFieldTypeLength = timeFieldType.getBytes(CHARSET).length;
		if (timeFieldTypeLength > MAX_LENGTH_TIME_FIELD_TYPE)
			throw new IllegalArgumentException(
					"错误的任务定义,<database>节点中<time-field>节点type属性超出最大长度"
							+ MAX_LENGTH_TIME_FIELD_TYPE + "，实际长度为"
							+ timeFieldTypeLength);
		dbSourceInfo.setTimeFieldType(timeFieldType);

		String timeFieldTable = timeFieldElement.attributeValue("table");
		int timeFieldTableLength = timeFieldTable.getBytes(CHARSET).length;
		if (timeFieldTableLength > MAX_LENGTH_MASTER_TABLE_NAME)
			throw new IllegalArgumentException(
					"错误的任务定义,<database>节点中<time-field>节点table属性超出最大长度"
							+ MAX_LENGTH_MASTER_TABLE_NAME + "，实际长度为"
							+ timeFieldTableLength);
		dbSourceInfo.setTimeFieldTable(timeFieldTable);

		// table-relation
		Element tableRelationElement = element.element("table-relation");
		if (tableRelationElement != null) {
			String tableRelation = tableRelationElement.getText();
			dbSourceInfo.setTableRelation(tableRelation);
		}

		// tables
		Element tablesElement = element.element("tables");

		// tables - table
		List<?> tableElementList = tablesElement.elements("table");
		boolean existsFlag = false;
		int keyCounter = 0;
		List<DbSourceTable> tableList = new LinkedList<DbSourceTable>();
		for (Object obj : tableElementList) {
			if (obj instanceof Element) {
				Element tableElement = (Element) obj;
				DbSourceTable tablePart = resolveDbSourceTable(tableElement);

				// 判断主键
				for (DbSourceField field : tablePart.getFieldList()) {
					if (field.isIndex()) {
						keyCounter++;
					}
				}

				if (!existsFlag)
					existsFlag = timeFieldTable.equalsIgnoreCase(tablePart
							.getName());

				tableList.add(tablePart);
			}
		}

		if (keyCounter < 1)
			throw new IllegalArgumentException(
					"错误的数据源定义,<table>节点中所有<field>子节点中必须至少有一个子节点is-index属性为true");

		// 时间字段time-field对应的表必须在table列表中
		if (!existsFlag)
			throw new IllegalArgumentException(
					"错误的数据源定义,<time-field>的table属性值必须在<tables>中存在");

		// 存在多个表[table/view/sql] table-relation不能为空
		if (tableList.size() > 1
				&& StringUtils.isBlank(dbSourceInfo.getTableRelation()))
			throw new IllegalArgumentException(
					"错误的数据源定义,当存在多个数据源时,<database>节点中<table-relation>子节点值不能为空");

		dbSourceInfo.setDbSourceTableList(tableList);

		return dbSourceInfo;
	}

	/**
	 * <pre>
	 * 解析文件数据源
	 * 
	 * @param element 数据源节点
	 * @return DB数据源信息
	 * @throws IllegalArgumentException 非法参数
	 * </pre>
	 */
	private static DbSourceInfo resolveFileDataSource(Element element) {
		DbSourceInfo dbSourceInfo = new DbSourceInfo();

		// table-relation
		Element tableRelationElement = element.element("relation");
		if (tableRelationElement != null) {
			String tableRelation = tableRelationElement.getText();
			dbSourceInfo.setTableRelation(tableRelation);
		}

		// data-files
		Element tablesElement = element.element("data-files");

		// data-files - data-file
		List<?> tableElementList = tablesElement.elements("data-file");
		int keyCounter = 0;
		List<DbSourceTable> tableList = new LinkedList<DbSourceTable>();
		for (Object obj : tableElementList) {
			if (obj instanceof Element) {
				Element tableElement = (Element) obj;
				DbSourceTable tablePart = resolveDataFile(tableElement);

				// 判断主键
				for (DbSourceField field : tablePart.getFieldList()) {
					if (field.isIndex()) {
						keyCounter++;
					}
				}

				tableList.add(tablePart);
			}
		}

		if (keyCounter < 1)
			throw new IllegalArgumentException(
					"错误的数据源定义,<data-file>节点中所有<field>子节点中必须至少有一个子节点is-index属性为true");

		// 存在多个文件时relation不能为空
		if (tableList.size() > 1
				&& StringUtils.isBlank(dbSourceInfo.getTableRelation()))
			throw new IllegalArgumentException(
					"错误的数据源定义,当存在多个数据源时,<file>节点中<relation>子节点值不能为空");

		dbSourceInfo.setDbSourceTableList(tableList);

		return dbSourceInfo;
	}

	/**
	 * <pre>
	 * 解析数据表
	 * 
	 * @param element 数据表节点
	 * @return 数据表实体类
	 * </pre>
	 */
	private static DbSourceTable resolveDbSourceTable(Element element) {
		DbSourceTable table = new DbSourceTable();

		// connection-id
		String connectionIdStr = element.attributeValue("connection-id");
		int connectionId = Integer.parseInt(connectionIdStr);
		table.setConnectionId(connectionId);

		// name
		String name = element.attributeValue("name");
		int nameLength = name.getBytes(CHARSET).length;
		if (nameLength > MAX_LENGTH_TABLE_NAME)
			throw new IllegalArgumentException("错误的数据源定义,<table>节点name属性超出最大长度"
					+ MAX_LENGTH_TABLE_NAME + "，实际长度为" + nameLength);
		table.setName(name);

		// sql
		Element sqlElement = element.element("sql");
		if (sqlElement != null) {
			String sql = sqlElement.getTextTrim();
			if (StringUtils.isBlank(sql))
				throw new IllegalArgumentException(
						"错误的数据源定义,<table>节点中存在<sql>子节点时,<sql>子节点内容不能为空");

			table.setSql(sql);
		}

		// field
		List<?> fieldElementList = element.elements("field");
		List<DbSourceField> fieldList = new LinkedList<DbSourceField>();
		for (Object obj : fieldElementList) {
			if (obj instanceof Element) {
				Element tableElement = (Element) obj;
				DbSourceField field = resolveDbSourceField(tableElement);

				fieldList.add(field);
			}
		}

		table.setFieldList(fieldList);

		return table;
	}

	/**
	 * <pre>
	 * 解析数据文件
	 * 
	 * @param element 数据文件节点
	 * @return 数据文件实体类
	 * </pre>
	 */
	private static DbSourceTable resolveDataFile(Element element) {
		DbSourceTable table = new DbSourceTable();

		// name
		String name = element.attributeValue("name");
		int nameLength = name.getBytes(CHARSET).length;
		if (nameLength > MAX_LENGTH_TABLE_NAME)
			throw new IllegalArgumentException(
					"错误的数据源定义,<data-file>节点name属性超出最大长度"
							+ MAX_LENGTH_TABLE_NAME + "，实际长度为" + nameLength);
		table.setName(name);

		// field
		List<?> fieldElementList = element.elements("field");
		List<DbSourceField> fieldList = new LinkedList<DbSourceField>();
		for (Object obj : fieldElementList) {
			if (obj instanceof Element) {
				Element tableElement = (Element) obj;
				DbSourceField field = resolveDbSourceField(tableElement);

				fieldList.add(field);
			}
		}

		table.setFieldList(fieldList);

		return table;
	}

	/**
	 * <pre>
	 * 解析数据源字段
	 * 
	 * @param element 字段节点
	 * @return 字段实体类
	 * </pre>
	 */
	private static DbSourceField resolveDbSourceField(Element element) {
		DbSourceField field = new DbSourceField();

		// name
		String name = element.attributeValue("name");
		int nameLength = name.getBytes(CHARSET).length;
		if (nameLength > MAX_LENGTH_FIELD_NAME)
			throw new IllegalArgumentException("错误的数据源定义,<field>节点name属性超出最大长度"
					+ MAX_LENGTH_FIELD_NAME + "，实际长度为" + nameLength);
		field.setName(name);

		// is-index
		String isIndexStr = element.attributeValue("is-index");
		boolean isIndex = false;
		if (StringUtils.isNotBlank(isIndexStr)) {
			isIndex = Boolean.parseBoolean(isIndexStr);
		}
		field.setIndex(isIndex);

		// is-export
		String isExportStr = element.attributeValue("is-export");
		boolean isExport = false;
		if (StringUtils.isNotBlank(isExportStr)) {
			isExport = Boolean.parseBoolean(isExportStr);
		}
		field.setExport(isExport);

		/*
		 * // 是否监控实体对象 String monitorEntity =
		 * element.attributeValue("is-export"); boolean isMonitorEntity = false;
		 * if (!StringUtils.isBlank(isExportStr)) { isMonitorEntity =
		 * Boolean.parseBoolean(monitorEntity); }
		 * field.setMonitorEntity(isMonitorEntity);
		 */

		return field;
	}

	/**
	 * <pre>
	 * 解析子任务节点 将所有子子任务节点信息解析转换为List<MonitorTask>
	 * 
	 * @param jobsElement 子任务节点
	 * @param fieldSet 数据源字段列表
	 * @param task MonitorTask
	 * @return 任务信息
	 * @throws Exception
	 * </pre>
	 */
	private static List<MonitorTask> resolveMonitorTaskJobs(
			Element jobsElement, Set<String> fieldSet, MonitorTask task)
			throws Exception {
		List<MonitorTask> taskList = new ArrayList<>();

		List<?> jobElementList = jobsElement.elements("job");
		for (Object obj : jobElementList) {
			if (!(obj instanceof Element))
				continue;

			Element jobElement = (Element) obj;
			MonitorTask _task = task.clone();

			// alarm-clear 可选属性,默认为false
			String alarmClear = jobElement.attributeValue("alarm-clear");
			if (StringUtils.isNotBlank(alarmClear)) {
				alarmClear = alarmClear.equals("false") ? "0" : "1";
				_task.setAlarmClear(Integer.parseInt(alarmClear));
			}

			// enable 可选属性，默认true
			String jobEnable = jobElement.attributeValue("enable");
			if (StringUtils.isNotBlank(jobEnable)) {
				_task.setJobEnable(BooleanUtils.toBoolean(jobEnable));
			}

			// reject 可选属性, 默认 false
			String reject = jobElement.attributeValue("reject");
			if (StringUtils.isNotBlank(reject)) {
				_task.setReject(BooleanUtils.toBoolean(reject));
			}

			// monitor-period-time
			/**
			 * 2013-09-12 避免程序中进行滑动窗口，固定窗口判断 从业务规则上看,仅有结单任务可以覆盖运行周期参数的功能
			 * 此处暂时兼容所有Job均能覆盖运行周期信息
			 */
			Element periodElement = jobElement.element("monitor-period-time");
			if (periodElement != null) {
				// 覆盖根节点monitor-period-time中unit和number属性
				String monitorPeriodUnit = periodElement.attributeValue("unit");
				String monitorPeriodNumberStr = periodElement
						.attributeValue("number");
				int monitorPeriodNumber = Integer
						.parseInt(monitorPeriodNumberStr);
				_task.setPeriodUnit(TimeUnit.valueOf(monitorPeriodUnit));
				_task.setPeriodNum(monitorPeriodNumber);
			}

			// monitor-filed
			String monitorField = jobElement.elementTextTrim("monitor-field");
			if (fieldSet != null && !fieldSet.contains(monitorField))
				throw new IllegalArgumentException(
						"错误的子任务定义,<monitor-field>节点值" + monitorField
								+ "的字段在数据表中未定义");
			int monitorFieldLength = monitorField.getBytes(CHARSET).length;
			if (monitorFieldLength > MAX_LENGTH_FIELD_NAME)
				throw new IllegalArgumentException(
						"错误的数据源定义,<monitor-field>节点值" + monitorField + "超出最大长度"
								+ MAX_LENGTH_FIELD_NAME + "，实际长度为"
								+ monitorFieldLength);
			_task.setMonitorField(monitorField);
			
			// first-monitor-time
			String firstMonitorTimeStr = jobElement.elementTextTrim("first-monitor-time");
			if (firstMonitorTimeStr != null) {
				// 覆盖根节点first-monitor-time属性
				Timestamp firstMonitorTime;
				try {
					Date date = TimeUtil.getDate(firstMonitorTimeStr,
							DEFAULT_DATE_TIME_FORMAT);
					firstMonitorTime = new Timestamp(date.getTime());
					_task.setCurrMonitorTime(firstMonitorTime);
				} catch (Exception e) {
					throw new IllegalArgumentException(
							"错误的<Job>定义,<first-monitor-time>节点值不是有效日期:yyyy-MM-ddTHH:mm:ss",
							e);
				}
			}
			
			// last-monitor-time
			String lastMonitorTimeStr = jobElement.elementTextTrim("last-monitor-time");
			if (lastMonitorTimeStr != null) {
				// 覆盖根节点last-monitor-time属性
				Timestamp lastMonitorTime;
				try {
					Date date = TimeUtil.getDate(lastMonitorTimeStr,
							DEFAULT_DATE_TIME_FORMAT);
					lastMonitorTime = new Timestamp(date.getTime());
					
					if (_task.getCurrMonitorTime().compareTo(lastMonitorTime) >= 0)
						throw new IllegalArgumentException(
								"错误的<Job>定义,<first-monitor-time>节点值必须早于<last-monitor-time>节点值");
					
					_task.setEndMonitorTime(lastMonitorTime);
				} catch (Exception e) {
					throw new IllegalArgumentException(
							"错误的<Job>定义,<last-monitor-time>节点值不是有效日期:yyyy-MM-ddTHH:mm:ss",
							e);
				}
			}

			// filter 可选节点
			Element filterElement = jobElement.element("filter");
			String filterContent = null;
			if (filterElement != null) {
				Filter filter = resolveFilter(filterElement, fieldSet);
				_task.setFilter(filter);

				filterContent = filterElement.asXML().trim();
			}
			_task.setFilterContent(filterContent);

			// expressions
			Element expressionsElement = jobElement.element("expressions");
			Expression expression = resolveExpressions(expressionsElement);
			_task.setExpression(expression);

			String expressionContent = expressionsElement.asXML().trim();
			_task.setExpressionContent(expressionContent);

			// rule
			Element ruleElement = jobElement.element("rule");
			Rule rule = resolveRule(ruleElement, _task);
			_task.setRule(rule);

			taskList.add(_task);
		}

		return taskList;
	}

	/**
	 * <pre>
	 * 解析数据过滤器
	 * 
	 * @param element 过滤规则节点
	 * @param fieldSet 表字段集合
	 * @return 过滤器实体类
	 * </pre>
	 */
	public static Filter resolveFilter(Element element, Set<String> fieldSet) {
		Filter filter = new Filter();

		// include
		List<Include> includeList = new ArrayList<>();
		List<?> includeElementList = element.elements("include");
		if (includeElementList != null && !includeElementList.isEmpty()) {
			for (Object obj : includeElementList) {
				if (obj instanceof Element) {
					Element item = (Element) obj;
					String field = item.attributeValue("field");
					String value = item.getText();

					if (StringUtils.isBlank(value))
						throw new IllegalArgumentException(
								"错误的数据源定义,<filter>节点中<include>子节点值为空");

					if (fieldSet != null && !fieldSet.contains(field))
						throw new IllegalArgumentException(
								"错误的数据源定义,<filter>节点中<include>子节点字段名为" + field
										+ "的字段在数据表中未定义");

					Include include = new Include(field, value);
					include.setPrototype(item.asXML());
					includeList.add(include);
				}
			}

			filter.setIncludeList(includeList);
		}

		// exclude
		List<Exclude> excludeList = new ArrayList<>();
		List<?> excludeElementList = element.elements("exclude");
		if (excludeElementList != null && !excludeElementList.isEmpty()) {
			for (Object obj : excludeElementList) {
				if (obj instanceof Element) {
					Element item = (Element) obj;
					String field = item.attributeValue("field");
					String value = item.getText();

					if (StringUtils.isBlank(value))
						throw new IllegalArgumentException(
								"错误的数据源定义,<filter>节点中<exclude>子节点值为空");

					if (fieldSet != null && !fieldSet.contains(field))
						throw new IllegalArgumentException(
								"错误的数据源定义,<filter>节点中<exclude>子节点字段名为" + field
										+ "的字段在数据表中未定义");

					Exclude exclude = new Exclude(field, value);
					exclude.setPrototype(item.asXML());
					excludeList.add(exclude);
				}
			}

			filter.setExcludeList(excludeList);
		}

		// includeSQL
		List<IncludeSql> includeSqlList = new ArrayList<>();
		List<?> includeSqlElementList = element.elements("includeSQL");
		if (includeSqlElementList != null && !includeSqlElementList.isEmpty()) {
			for (Object obj : includeSqlElementList) {
				if (obj instanceof Element) {
					Element item = (Element) obj;
					String field = item.attributeValue("field");
					String connectionIdStr = item
							.attributeValue("connection-id");
					int connectionId = Integer.parseInt(connectionIdStr);
					String value = item.getText();

					if (StringUtils.isBlank(value))
						throw new IllegalArgumentException(
								"错误的数据源定义,<filter>节点中<includeSQL>子节点值为空");

					if (fieldSet != null && !fieldSet.contains(field))
						throw new IllegalArgumentException(
								"错误的数据源定义,<filter>节点中<includeSQL>子节点字段名为"
										+ field + "的字段在数据表中未定义");

					IncludeSql includeSql = new IncludeSql(field, value,
							connectionId);
					includeSql.setPrototype(item.asXML());
					includeSqlList.add(includeSql);
				}
			}

			filter.setIncludeSqlList(includeSqlList);
		}

		// excludeSql
		List<ExcludeSql> excludeSqlList = new ArrayList<>();
		List<?> excludeSqlElementList = element.elements("excludeSQL");
		if (excludeSqlElementList != null && !excludeSqlElementList.isEmpty()) {
			for (Object obj : excludeSqlElementList) {
				if (obj instanceof Element) {
					Element item = (Element) obj;
					String field = item.attributeValue("field");
					String connectionIdStr = item
							.attributeValue("connection-id");
					int connectionId = Integer.parseInt(connectionIdStr);
					String value = item.getText();

					if (StringUtils.isBlank(value))
						throw new IllegalArgumentException(
								"错误的数据源定义,<filter>节点中<excludeSQL>子节点值为空");

					if (fieldSet != null && !fieldSet.contains(field))
						throw new IllegalArgumentException(
								"错误的数据源定义,<filter>节点中<excludeSQL>子节点字段名为"
										+ field + "的字段在数据表中未定义");

					ExcludeSql excludeSql = new ExcludeSql(field, value,
							connectionId);
					excludeSql.setPrototype(item.asXML());
					excludeSqlList.add(excludeSql);
				}
			}

			filter.setExcludeSqlList(excludeSqlList);
		}

		// busy-hour
		Element busyHour = element.element("busy-hour");
		if (busyHour != null) {
			if (busyHour.attributeCount() != 2
					|| (StringUtils
							.isNotBlank(busyHour.attributeValue("field")) && StringUtils
							.isNotBlank(busyHour.attributeValue("level")))
					|| (StringUtils
							.isNotBlank(busyHour.attributeValue("field")) && StringUtils
							.isNotBlank(busyHour.attributeValue("type")))
					|| (StringUtils
							.isNotBlank(busyHour.attributeValue("value")) && StringUtils
							.isNotBlank(busyHour.attributeValue("level")))
					|| (StringUtils
							.isNotBlank(busyHour.attributeValue("value")) && StringUtils
							.isNotBlank(busyHour.attributeValue("type"))))
				throw new IllegalArgumentException(
						"错误的数据源定义,<busy-hour>节点属性定义无效，只能field与value属性或level与type属性同时存在");

			// TODO 忙时的数据解析(-)
		}

		// holiday
		Element holidayElement = element.element("holiday");
		if (holidayElement != null) {
			// policy
			String policyStr = holidayElement.attributeValue("policy");
			Integer policy = Integer.parseInt(policyStr);
			if (policy < 0 || policy > 2)
				throw new IllegalArgumentException(
						"错误的数据源定义,<filter>节点中<holiday>子节点policy属性值不合法");

			// strategy
			String strategyStr = holidayElement.attributeValue("strategy");
			Integer strategy = Integer.parseInt(strategyStr);
			if (strategy < 0 || strategy > 2)
				throw new IllegalArgumentException(
						"错误的数据源定义,<filter>节点中<holiday>子节点strategy属性值不合法");

			Holiday holiday = new Holiday(policy, strategy);
			filter.setHoliday(holiday);
		}

		return filter;
	}

	/**
	 * <pre>
	 * 解析表达式信息
	 * 
	 * @param expressionsElement
	 * @throws Exception xml非法或者校验不通过
	 * </pre>
	 */
	public static Expression resolveExpressions(Element expressionsElement)
			throws Exception {
		Element element = expressionsElement.element("data-range");
		DataRange dataRange = resolveDataRange(element);

		List<?> indicatorElements = expressionsElement.elements("indicator");
		List<Indicator> indicatorList = resolveIndicator(indicatorElements);

		Expression expression = new Expression();
		expression.setDataRange(dataRange);
		expression.setIndicatorList(indicatorList);

		return expression;
	}

	/**
	 * <pre>
	 * 解析规则配置信息
	 * 
	 * @param ruleElement
	 * @param task MonitorTask
	 * @throws IllegalArgumentException xml非法或者校验不通过
	 * </pre>
	 */
	public static Rule resolveRule(Element ruleElement, MonitorTask task) {
		Rule rule = new Rule();

		// rule - description
		Element descriptionElement = ruleElement.element("description");
		if (descriptionElement != null) {
			String description = descriptionElement.getTextTrim();
			int descriptionLen = description.getBytes(CHARSET).length;
			if (descriptionLen > MAX_LENGTH_RULE_DESCRIPTION)
				throw new IllegalArgumentException(
						"错误的子任务定义,规则信息描述节点<description>内容超过最大长度"
								+ MAX_LENGTH_RULE_DESCRIPTION + "，实际长度为"
								+ descriptionLen);

			task.setRuleDescription(description);
		}

		Element _element = null;

		// period-info
		_element = ruleElement.element("period-info");
		PeriodInfo periodInfo = resolvePeriod(_element);
		rule.setPeriodInfo(periodInfo);

		String periodInfoXml = _element.asXML().trim();
		int periodInfoLen = periodInfoXml.getBytes(CHARSET).length;
		if (periodInfoLen > MAX_LENGTH_RULE_PERIOD_INFO)
			throw new IllegalArgumentException(
					"错误的子任务定义,周期信息节点<period-info>内容超过最大长度"
							+ MAX_LENGTH_RULE_PERIOD_INFO + "，实际长度为"
							+ periodInfoLen);
		task.setPeriodInfoContent(periodInfoXml);

		// top-info
		_element = ruleElement.element("top-info");
		if (_element != null) {
			Top top = resolveTopN(_element);
			rule.setTop(top);

			String topInfoXml = _element.asXML().trim();
			int topInfoLen = topInfoXml.getBytes(CHARSET).length;
			if (StringUtils.isNotBlank(topInfoXml)
					&& topInfoLen > MAX_LENGTH_RULE_TOP_INFO)
				throw new IllegalArgumentException(
						"错误的子任务定义,TOPN节点<top-info>内容超过最大长度"
								+ MAX_LENGTH_RULE_TOP_INFO + "，实际长度为"
								+ topInfoLen);
			task.setTopContent(topInfoXml);
		}

		// alarm-level
		_element = ruleElement.element("alarm-level");
		if (_element != null) {
			AlarmLevel alarmLevel = resolveAlarmLevel(_element);
			rule.setAlarmLevel(alarmLevel);

			String alarmLevelXml = _element.asXML().trim();
			int alarmLen = alarmLevelXml.getBytes(CHARSET).length;
			if (StringUtils.isNotBlank(alarmLevelXml)
					&& alarmLen > MAX_LENGTH_RULE_ALARM_LEVEL)
				throw new IllegalArgumentException(
						"错误的子任务定义,告警级别节点<alarm-level>内容超过最大长度"
								+ MAX_LENGTH_RULE_ALARM_LEVEL + "，实际长度为"
								+ alarmLen);
			task.setAlarmLevelContent(alarmLevelXml);
		}

		return rule;
	}

	/**
	 * <pre>
	 * 解析DataRange
	 * 
	 * @param element DataRange节点
	 * @return
	 * </pre>
	 */
	public static DataRange resolveDataRange(Element element) {
		DataRange dataRange = new DataRange();
		List<Range> rangeList = new ArrayList<>();

		List<?> rangeListObj = element.elements("range");
		for (Object rangeObj : rangeListObj) {
			if (rangeObj instanceof Element) {
				Element rangeElement = (Element) rangeObj;
				String alias = rangeElement.attributeValue("alias");

				/*
				 * 2013-08-30 Chris 确认过如果range只有一个,且表达式中未使用别名,则别名属性可以不定义 if
				 * (StringUtil.isNull(alias)) throw new
				 * IllegalArgumentException("错误的指标定义.<range>节点定义别名为空.");
				 */

				Range range = new Range(alias);
				List<?> offsetElements = rangeElement.elements("offset");
				ArrayList<Offset> offsetList = new ArrayList<>();
				offsetList.ensureCapacity(offsetElements.size());
				for (Object offsetObj : offsetElements) {
					if (offsetObj instanceof Element) {
						Element offset = (Element) offsetObj;
						String fromMinute = offset.attributeValue("from");
						String toMinute = offset.attributeValue("to");
						Offset rangeTimeOffset = new Offset();
						rangeTimeOffset.setFrom(Integer.parseInt(fromMinute));
						rangeTimeOffset.setTo(Integer.parseInt(toMinute));
						offsetList.add(rangeTimeOffset);
					}
				}
				range.setOffsetList(offsetList);
				rangeList.add(range);
			}
		}

		dataRange.setRangeList(rangeList);

		return dataRange;
	}

	/**
	 * <pre>
	 * 解析表达式
	 * 
	 * @param indicatorElements 表达式节点
	 * @return
	 * @throws Exception
	 * </pre>
	 */
	public static List<Indicator> resolveIndicator(List<?> indicatorElements)
			throws Exception {
		List<Indicator> indicatorList = new LinkedList<Indicator>();
		for (Object indicatorObj : indicatorElements) {
			if (indicatorObj instanceof Element) {
				Element indicatorElement = (Element) indicatorObj;
				String alarmLevel = indicatorElement
						.attributeValue("alarm-level");
				String expression = indicatorElement.getTextTrim();
				// Chris，2015.01.23，应前端要求，当传递的表达式为空值不做检测，允许保存。PS：这样做极不合理！
				if (StringUtils.isNotBlank(expression)) {
					// 指标表达式语法校验
					FormulaExpression.valid(expression);
				}

				Indicator indicator = new Indicator();
				indicator.setAlarmLevel(Short.parseShort(alarmLevel));
				indicator.setContent(expression);
				indicatorList.add(indicator);
			}
		}

		return indicatorList;
	}

	/**
	 * <pre>
	 * 处理规则配置中的周期信息
	 * 
	 * @param periodElement 周期信息节点
	 * @return PeriodInfo 周期信息
	 * </pre>
	 */
	public static PeriodInfo resolvePeriod(Element periodElement) {
		// analysis-period
		Element analysisElement = periodElement.element("analysis-period");
		String unit = analysisElement.attributeValue("unit");
		String periodNum = analysisElement.attributeValue("period-num");
		AnalysisPeriod analysisPeriod = new AnalysisPeriod(
				TimeUnit.valueOf(unit), Integer.valueOf(periodNum));

		// monitor-period
		Element monitorPeriodElement = periodElement.element("monitor-period");
		unit = monitorPeriodElement.attributeValue("unit");
		periodNum = monitorPeriodElement.attributeValue("period-num");
		// 兼容原接口，如果没有此字段，使用默认值1代替
		String needWhole = monitorPeriodElement.attributeValue("need-whole");
		if(null == needWhole){
			needWhole = "1";
		}
		String assignMonitorTime = monitorPeriodElement
				.attributeValue("assign-monitor-time");
		MonitorPeriod monitorPeriod = new MonitorPeriod(TimeUnit.valueOf(unit),
				Integer.valueOf(periodNum),Integer.valueOf(needWhole), assignMonitorTime);

		// monitor-period -> assign-time
		Element assignTimeElement = monitorPeriodElement.element("assign-time");
		if (assignTimeElement != null) {
			String assignUnit = assignTimeElement.attributeValue("unit");
			String monitorAssignTimes = assignTimeElement
					.attributeValue("value");
			monitorPeriod.setAssignTime(new AssignTime(TimeUnit
					.valueOf(assignUnit), monitorAssignTimes));
		}

		// occur-times
		OccurTimes occurTimes = null;
		Element occurTimesElement = periodElement.element("occur-times");
		if (occurTimesElement != null) {
			String occurTimeUnit = occurTimesElement.attributeValue("unit");
			String occurTimesValue = occurTimesElement.attributeValue("value");
			String continues = occurTimesElement.attributeValue("continues");
			occurTimes = new OccurTimes(TimeUnit.valueOf(occurTimeUnit),
					Integer.parseInt(occurTimesValue),
					Boolean.valueOf(continues));

			// monitor-occur-time 2013-09-16
			Element monitorOccourTimeElement = occurTimesElement
					.element("monitor-occur-times");
			if (monitorOccourTimeElement != null) {
				String monitorOccurTimesValue = monitorOccourTimeElement
						.attributeValue("value");
				String monitorOccurTimeUnit = monitorOccourTimeElement
						.attributeValue("unit");
				String monitorContinues = monitorOccourTimeElement
						.attributeValue("continues");
				SubOccurTimes monitorOccurTimes = new SubOccurTimes(
						TimeUnit.valueOf(monitorOccurTimeUnit),
						Integer.parseInt(monitorOccurTimesValue),
						Boolean.valueOf(monitorContinues));
				occurTimes.setSubOccurTimes(monitorOccurTimes);
			}
		}

		PeriodInfo periodInfo = new PeriodInfo(analysisPeriod, monitorPeriod,
				occurTimes);
		return periodInfo;
	}

	/**
	 * <pre>
	 * 解析规则中的TOPN信息 TOPN是可选节点，但是一旦存在需要进行校验
	 * 
	 * @param periodInfo 周期信息节点
	 * @return Top信息
	 * </pre>
	 */
	public static Top resolveTopN(Element topElement) {
		String topNumber = topElement.attributeValue("number");
		Top top = new Top(Integer.parseInt(topNumber));

		// 当前仅支持一个排序表达式(-)
		Element orderIndicatorElement = topElement.element("order-indicator");
		String sortWay = orderIndicatorElement.attributeValue("sort-way");
		String orderBy = orderIndicatorElement.getTextTrim();
		OrderIndicator orderIndicator = new OrderIndicator(
				SortWay.valueOf(sortWay), orderBy);
		top.setOrderIndicator(orderIndicator);

		// 支持多种排序方式
		// List<Element> orderExpressionElements =
		// topElement.elements("order-expression");
		//
		// if ((orderIndicatorElements == null ||
		// orderIndicatorElements.isEmpty())
		// && (orderExpressionElements == null ||
		// orderExpressionElements.isEmpty()))
		// throw new
		// IllegalArgumentException("错误的规则定义,<top-info>节点中<order-indicator>和<order-expression>必须至少存在一种");
		//
		// List<OrderIndicator> oiList = new ArrayList<>();
		// for (Element item : orderIndicatorElements) {
		// String sortWay = item.attributeValue("sort-way");
		// if (StringUtils.isBlank(sortWay) ||
		// !("DESC".equalsIgnoreCase(sortWay) ||
		// "ASC".equalsIgnoreCase(sortWay)))
		// throw new
		// IllegalArgumentException("错误的规则定义,<top-info>节点中<order-indicator>节点sort-way属性为空或无法识别："
		// + sortWay);
		// String orderBy = item.getTextTrim();
		// if (StringUtils.isBlank(orderBy))
		// throw new
		// IllegalArgumentException("错误的规则定义,<top-info>节点中<order-indicator>内容为空");
		// OrderIndicator oi = monitorRule.new OrderIndicator(sortWay, orderBy);
		// oiList.add(oi);
		// }
		// monitorRule.setOrderIndicatorList(oiList);
		//
		// List<OrderExpression> oeList = new ArrayList<>();
		// for (Element item : orderExpressionElements) {
		// String sortWay = item.attributeValue("sort-way");
		// if (StringUtils.isBlank(sortWay) ||
		// !("DESC".equalsIgnoreCase(sortWay) ||
		// "ASC".equalsIgnoreCase(sortWay)))
		// throw new
		// IllegalArgumentException("错误的规则定义,<top-info>节点中<order-expression>节点sort-way属性为空或无法识别："
		// + sortWay);
		// String oderBy = item.getTextTrim();
		// if (StringUtils.isBlank(oderBy))
		// throw new
		// IllegalArgumentException("错误的规则定义,<top-info>节点中<order-expression>内容为空");
		// OrderExpression oe = monitorRule.new OrderExpression(sortWay,
		// oderBy);
		// oeList.add(oe);
		// }
		// monitorRule.setOrderExpressionList(oeList);

		return top;
	}

	/**
	 * <pre>
	 * 解析规则中的告警级别定义 告警级别定义是可选节点，一旦定义需要进行校验
	 * 
	 * @param periodInfo 周期信息节点
	 * @return 告警级别信息
	 * </pre>
	 */
	public static AlarmLevel resolveAlarmLevel(Element alarmLevelElement) {
		String defaultLevelStr = alarmLevelElement.attributeValue("default");
		short defaultLevel = Short.parseShort(defaultLevelStr);
		AlarmLevel alarmLevel = new AlarmLevel(defaultLevel);

		List<?> alarmObj = alarmLevelElement.elements("alarm");
		ArrayList<Alarm> alarmList = new ArrayList<>();
		alarmList.ensureCapacity(alarmObj.size());
		// 记录告警次数 记录最后一次
		Set<Integer> countSetUp = new HashSet<Integer>();
		countSetUp.add(Integer.MAX_VALUE);
		// 告警级别
		short level = 0;
		for (Object obj : alarmObj) {
			// 待详查解析告警级别定义的代码
			if (obj instanceof Element) {
				Element alarmElement = (Element) obj;
				String $levelStr = alarmElement.attributeValue("level");
				short $level = Short.parseShort($levelStr);
				// 出现频次校验 频次 上一个比下一个次数要大，级别 上一个比下一个更高
				if ($level < level)
					throw new IllegalArgumentException(
							"错误的规则定义,<alarm-level>子节点<alarm>中level不能比前一个级别更高");

				level = $level;

				// 配置的事件次数
				String occurTimes = alarmElement.attributeValue("occur_times");
				Set<Integer> countSet = new HashSet<Integer>();
				for (String count : occurTimes.split(",")) {
					for (Integer countUp : countSetUp) {
						if (countUp.compareTo(Integer.valueOf(count)) < 0)
							throw new IllegalArgumentException(
									"错误的规则定义,<alarm-level>子节点<alarm>中occur_times不能比前一个次数数值大");
					}
					countSet.add(Integer.valueOf(count));
				}

				countSetUp = countSet;

				Alarm alarm = alarmLevel.new Alarm(level, occurTimes);
				alarmList.add(alarm);
			}
		}

		if (defaultLevel < level)
			throw new IllegalArgumentException(
					"错误的规则定义,<alarm-level>子节点default级别配置不能比<alarm>中level级别更高");

		alarmLevel.setAlarmList(alarmList);

		return alarmLevel;
	}

	/**
	 * <pre>
	 * 通过XSD校验安全校验信息
	 * 
	 * @param is 安全校验数据流
	 * @throws SAXException
	 * @throws IOException
	 * </pre>
	 */
	private static void validationSecure(InputStream is) throws SAXException,
			IOException {
		Source xml = new StreamSource(is);
		validation(URL_UPORT_SECURE_VALIDATE, xml);
	}

	/**
	 * <pre>
	 * 通过XSD校验监控任务
	 * 
	 * @param xml 监控任务数据流
	 * @throws SAXException
	 * @throws IOException
	 * </pre>
	 */
	private static void validationTask(InputStream is) throws SAXException,
			IOException {
		Source xml = new StreamSource(is);
		validation(URL_UBP_MONITOR_TASK, xml);
	}

	/**
	 * <pre>
	 * 通过XSD校验XML
	 * 
	 * @param xsd XSD文件
	 * @param xml XML信息
	 * @throws SAXException
	 * @throws IOException
	 * </pre>
	 */
	private static void validation(URL xsd, Source xml) throws SAXException,
			IOException {
		SchemaFactory sf = SchemaFactory
				.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		Validator validator = sf.newSchema(xsd).newValidator();
		validator.validate(xml);
	}

}
