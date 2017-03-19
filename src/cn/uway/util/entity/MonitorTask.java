package cn.uway.util.entity;

import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import cn.uway.framework.util.DateTimeUtil;
import cn.uway.util.Resolver;
import cn.uway.util.entity.AlarmLevel.Alarm;
import cn.uway.util.enums.TimeUnit;

/**
 * 监控任务实体类
 * 
 * @author Chris 2013-11-5
 */
public class MonitorTask implements Cloneable {

	/**
	 * 任务Id
	 */
	private long taskId;

	/**
	 * 任务名称 对应task表task_name字段
	 */
	private String taskName;

	/**
	 * 任务描述信息。
	 */
	private String taskDescription;

	/**
	 * 业务系统中的监控任务ID 对应task表monitor_task_id字段
	 */
	private String monitorTaskId;

	/**
	 * 系统编号，公司统一维护 对应task表caller_id字段
	 */
	private int callerId;

	/**
	 * 是否立即启用监控任务 对应task表is_used字段 默认为启用状态
	 */
	private boolean isUsed = true;

	/**
	 * 是否被删除
	 */
	private boolean isDeleted = false;

	/**
	 * 是否周期性任务
	 */
	private boolean isPeriod = true;

	/**
	 * 监控任务执行频率 对应task表period_num字段
	 */
	private int periodNum;

	/**
	 * 监控任务运行频率单位 对应task表period_unit字段
	 */
	private TimeUnit periodUnit;

	/**
	 * 数据源Id
	 */
	private long dataSourceId;

	/**
	 * 当前监控时间 对应task表curr_monitor_time字段
	 */
	private Timestamp currMonitorTime;

	/**
	 * 监控截止时间 对应task表end_ monitor_time字段 本节点可选
	 */
	private Timestamp endMonitorTime;

	/**
	 * 任务分组ID
	 */
	protected int groupId;

	/**
	 * 运行机器名
	 */
	protected String pcName;

	/**
	 * 数据地区 对应task表CITY_ID字段
	 */
	private int cityId;

	/**
	 * 任务有效性
	 */
	private boolean isValid;

	/**
	 * 主指标名称 对应task表PI_NAME字段 本节点可选，联通用
	 */
	private String piName;

	/**
	 * 主指标描述 对应task表PI_EXPR_DESCRIPTION字段 本节点可选，联通用
	 */
	private String piExprDescription;

	/**
	 * 对象标识类型 对应task表key_index_type字段 本节点可选，电信用
	 */
	private Integer keyIndexType;

	/**
	 * 对象标识类型 对应task表t_id字段 本节点可选，电信用
	 */
	private Integer tId;

	/**
	 * 是否告警清除
	 */
	private int isAlarmClear;

	/**
	 * 监控字段
	 */
	private String monitorField;

	/**
	 * 数据源信息表对象
	 */
	private DataSource dataSource;

	/**
	 * 监控子任务过滤器
	 */
	private Filter filter;

	/**
	 * 指标
	 */
	private Expression expression;

	/**
	 * 规则
	 */
	private Rule rule;

	/**
	 * 过滤器定义内容
	 */
	private String filterContent;

	/**
	 * 表达式定义内容
	 */
	private String expressionContent;

	/**
	 * 周期定义内容
	 */
	private String periodInfoContent;

	/**
	 * Top定义内容
	 */
	private String topContent;

	/**
	 * 告警级别定义内容
	 */
	private String alarmLevelContent;

	/**
	 * 规则信息描述 可选节点，联通用
	 */
	private String ruleDescription;

	/**
	 * 任务预留信息字段
	 */
	private String memo;

	private static final String CRLF = "\r\n";

	private static final String TAB = "\t";

	public boolean isReject() {
		return reject;
	}

	public void setReject(boolean reject) {
		this.reject = reject;
	}

	public boolean isJobEnable() {
		return jobEnable;
	}

	public void setJobEnable(boolean jobEnable) {
		this.jobEnable = jobEnable;
	}

	private boolean reject = false;

	private boolean jobEnable = true;

	public MonitorTask() {

	}

	public MonitorTask(long taskId, String taskName, String taskDescription,
			String monitorTaskId, int callerId, int isUsed, int isDeleted,
			int isPeriod, int periodNum, String periodUnit, long dataSourceId,
			Timestamp currMonitorTime, Timestamp endMonitorTime, int groupId,
			String pcName, int cityId, int isValid, String piName,
			String piExprDescription, String filterContent,
			Integer keyIndexType, Integer tId, int isAlarmClear,
			String monitorField, String expressionContent,
			String periodInfoContent, String topContent,
			String alarmLevelContent, String ruleDescription, String memo)
			throws Exception {
		super();
		this.taskId = taskId;
		this.taskName = taskName;
		this.taskDescription = taskDescription;
		this.monitorTaskId = monitorTaskId;
		this.callerId = callerId;
		this.isUsed = isUsed == 1 ? true : false;
		this.isDeleted = isDeleted == 1 ? true : false;
		this.isPeriod = isPeriod == 1 ? true : false;
		this.periodNum = periodNum;
		this.periodUnit = TimeUnit.valueOf(periodUnit);
		this.dataSourceId = dataSourceId;
		this.currMonitorTime = currMonitorTime;
		this.endMonitorTime = endMonitorTime;
		this.groupId = groupId;
		this.pcName = pcName;
		this.cityId = cityId;
		this.isValid = isValid == 1 ? true : false;
		this.piName = piName;
		this.piExprDescription = piExprDescription;
		this.filterContent = filterContent;
		this.keyIndexType = keyIndexType;
		this.tId = tId;
		this.isAlarmClear = isAlarmClear;
		this.monitorField = monitorField;
		this.expressionContent = expressionContent;
		this.periodInfoContent = periodInfoContent;
		this.topContent = topContent;
		this.alarmLevelContent = alarmLevelContent;
		this.ruleDescription = ruleDescription;
		this.memo = memo;

		// 解析过滤器
		if (StringUtils.isNotBlank(filterContent)) {
			Document doc = DocumentHelper.parseText(filterContent);
			Element element = doc.getRootElement();
			filter = Resolver.resolveFilter(element, null);
		}

		// 解析表达式
		if (StringUtils.isNotBlank(expressionContent)) {
			Document doc = DocumentHelper.parseText(expressionContent);
			Element element = doc.getRootElement();
			expression = Resolver.resolveExpressions(element);
		}

		Document doc;
		Element element;
		// 解析规则：period-info
		doc = DocumentHelper.parseText(periodInfoContent);
		element = doc.getRootElement();
		PeriodInfo periodInfo = Resolver.resolvePeriod(element);

		// 解析规则：top-info
		Top top = null;
		if (StringUtils.isNotBlank(topContent)) {
			doc = DocumentHelper.parseText(topContent);
			element = doc.getRootElement();
			top = Resolver.resolveTopN(element);
		}

		// 解析规则：alarm-level
		AlarmLevel alarmLevel = null;
		if (StringUtils.isNotBlank(alarmLevelContent)) {
			doc = DocumentHelper.parseText(alarmLevelContent);
			element = doc.getRootElement();
			alarmLevel = Resolver.resolveAlarmLevel(element);
		}

		rule = new Rule(periodInfo, top, alarmLevel);
	}

	public long getTaskId() {
		return taskId;
	}

	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

	public String getTaskName() {
		return taskName;
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public String getTaskDescription() {
		return taskDescription;
	}

	public void setTaskDescription(String taskDescription) {
		this.taskDescription = taskDescription;
	}

	public String getMonitorTaskId() {
		return monitorTaskId;
	}

	public void setMonitorTaskId(String monitorTaskId) {
		this.monitorTaskId = monitorTaskId;
	}

	public int getCallerId() {
		return callerId;
	}

	public void setCallerId(int callerId) {
		this.callerId = callerId;
	}

	public boolean isUsed() {
		return isUsed;
	}

	public void setUsed(boolean isUsed) {
		this.isUsed = isUsed;
	}

	public boolean isDeleted() {
		return isDeleted;
	}

	public void setDeleted(boolean isDeleted) {
		this.isDeleted = isDeleted;
	}

	public boolean isPeriod() {
		return isPeriod;
	}

	public void setPeriod(boolean isPeriod) {
		this.isPeriod = isPeriod;
	}

	public int getPeriodNum() {
		return periodNum;
	}

	public void setPeriodNum(int periodNum) {
		this.periodNum = periodNum;
	}

	public TimeUnit getPeriodUnit() {
		return periodUnit;
	}

	public void setPeriodUnit(TimeUnit periodUnit) {
		this.periodUnit = periodUnit;
	}

	public long getDataSourceId() {
		return dataSourceId;
	}

	public void setDataSourceId(long dataSourceId) {
		this.dataSourceId = dataSourceId;
	}

	public Timestamp getCurrMonitorTime() {
		return currMonitorTime;
	}

	public void setCurrMonitorTime(Timestamp currMonitorTime) {
		this.currMonitorTime = currMonitorTime;
	}

	public Timestamp getEndMonitorTime() {
		return endMonitorTime;
	}

	public void setEndMonitorTime(Timestamp endMonitorTime) {
		this.endMonitorTime = endMonitorTime;
	}

	public int getGroupId() {
		return groupId;
	}

	public void setGroupId(int groupId) {
		this.groupId = groupId;
	}

	public String getPcName() {
		return pcName;
	}

	public void setPcName(String pcName) {
		this.pcName = pcName;
	}

	public int getCityId() {
		return cityId;
	}

	public void setCityId(int cityId) {
		this.cityId = cityId;
	}

	public boolean isValid() {
		return isValid;
	}

	public void setValid(boolean isValid) {
		this.isValid = isValid;
	}

	public String getPiName() {
		return piName;
	}

	public void setPiName(String piName) {
		this.piName = piName;
	}

	public String getPiExprDescription() {
		return piExprDescription;
	}

	public void setPiExprDescription(String piExprDescription) {
		this.piExprDescription = piExprDescription;
	}

	public Integer getKeyIndexType() {
		return keyIndexType;
	}

	public void setKeyIndexType(Integer keyIndexType) {
		this.keyIndexType = keyIndexType;
	}

	public Integer gettId() {
		return tId;
	}

	public void settId(Integer tId) {
		this.tId = tId;
	}

	public int getAlarmClear() {
		return isAlarmClear;
	}

	public void setAlarmClear(int isAlarmClear) {
		this.isAlarmClear = isAlarmClear;
	}

	public String getMonitorField() {
		return monitorField;
	}

	public void setMonitorField(String monitorField) {
		this.monitorField = monitorField;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public Filter getFilter() {
		return filter;
	}

	public void setFilter(Filter filter) {
		this.filter = filter;
	}

	public Expression getExpression() {
		return expression;
	}

	public void setExpression(Expression expression) {
		this.expression = expression;
	}

	public Rule getRule() {
		return rule;
	}

	public void setRule(Rule rule) {
		this.rule = rule;
	}

	public String getFilterContent() {
		return filterContent;
	}

	public void setFilterContent(String filterContent) {
		this.filterContent = filterContent;
	}

	public String getExpressionContent() {
		return expressionContent;
	}

	public void setExpressionContent(String expressionContent) {
		this.expressionContent = expressionContent;
	}

	public String getPeriodInfoContent() {
		return periodInfoContent;
	}

	public void setPeriodInfoContent(String periodInfoContent) {
		this.periodInfoContent = periodInfoContent;
	}

	public String getTopContent() {
		return topContent;
	}

	public void setTopContent(String topContent) {
		this.topContent = topContent;
	}

	public String getAlarmLevelContent() {
		return alarmLevelContent;
	}

	public void setAlarmLevelContent(String alarmLevelContent) {
		this.alarmLevelContent = alarmLevelContent;
	}

	public String getRuleDescription() {
		return ruleDescription;
	}

	public void setRuleDescription(String ruleDescription) {
		this.ruleDescription = ruleDescription;
	}

	public String getMemo() {
		return memo;
	}

	public void setMemo(String memo) {
		this.memo = memo;
	}

	public String toXML() {
		StringBuilder xmlBuilder = new StringBuilder();
		xmlBuilder.append("<task>").append(CRLF);

		xmlBuilder.append(TAB).append("<task-id>").append(taskId)
				.append("</task-id>").append(CRLF);
		xmlBuilder.append(TAB).append("<monitor-task-id>")
				.append(monitorTaskId).append("</monitor-task-id>")
				.append(CRLF);
		xmlBuilder.append(TAB).append("<monitor-task-name>")
				.append("<![CDATA[").append(taskName).append("]]>")
				.append("</monitor-task-name>").append(CRLF);
		// xmlBuilder.append(TAB).append("<caller-id>").append(callerId).append("</caller-id>").append(CRLF);
		xmlBuilder.append(TAB).append("<task-description>").append("<![CDATA[")
				.append(taskDescription).append("]]>")
				.append("</task-description>").append(CRLF);
		xmlBuilder.append(TAB).append("<is-used>").append(isUsed)
				.append("</is-used>").append(CRLF);
		xmlBuilder.append(TAB).append("<is-deleted>").append(isDeleted)
				.append("</is-deleted>").append(CRLF);
		xmlBuilder.append(TAB).append("<is-period>").append(isPeriod)
				.append("</is-period>").append(CRLF);
		xmlBuilder.append(TAB).append("<period-num>").append(periodNum)
				.append("</period-num>").append(CRLF);
		xmlBuilder.append(TAB).append("<period-unit>").append(periodUnit)
				.append("</period-unit>").append(CRLF);
		xmlBuilder.append(TAB).append("<datasource-id>").append(dataSourceId)
				.append("</datasource-id>").append(CRLF);
		xmlBuilder.append(TAB).append("<curr-monitor-time>")
				.append(DateTimeUtil.formatDateTime(currMonitorTime))
				.append("</curr-monitor-time>").append(CRLF);
		xmlBuilder.append(TAB).append("<end-monitor-time>")
				.append(DateTimeUtil.formatDateTime(endMonitorTime))
				.append("</end-monitor-time>").append(CRLF);
		// xmlBuilder.append(TAB).append("<group-id>").append(groupId).append("</group-id>").append(CRLF);
		// xmlBuilder.append(TAB).append("<pc-name>").append(pcName).append("</pc-name>").append(CRLF);
		xmlBuilder.append(TAB).append("<city-id>").append(cityId)
				.append("</city-id>").append(CRLF);
		xmlBuilder.append(TAB).append("<is-valid>").append(isValid)
				.append("</is-valid>").append(CRLF);
		xmlBuilder.append(TAB).append("<pi-name>").append("<![CDATA[")
				.append(piName).append("]]>").append("</pi-name>").append(CRLF);
		xmlBuilder.append(TAB).append("<pi-expr-description>")
				.append("<![CDATA[").append(piExprDescription).append("]]>")
				.append("</pi-expr-description>").append(CRLF);
		xmlBuilder.append(TAB).append("<filter>").append(filter)
				.append("</filter>").append(CRLF);
		xmlBuilder.append(TAB).append("<key-index-type>").append(keyIndexType)
				.append("</key-index-type>").append(CRLF);
		xmlBuilder.append(TAB).append("<t-id>").append(tId).append("</t-id>")
				.append(CRLF);
		xmlBuilder.append(TAB).append("<is-alarm-clear>").append(isAlarmClear)
				.append("</is-alarm-clear>").append(CRLF);
		xmlBuilder.append(TAB).append("<monitor-field>").append(monitorField)
				.append("</monitor-field>").append(CRLF);
		xmlBuilder.append(TAB).append("<expression-info>")
				.append(expressionContent).append("</expression-info>")
				.append(CRLF);
		xmlBuilder.append(TAB).append("<period-info>")
				.append(periodInfoContent).append("</period-info>")
				.append(CRLF);
		xmlBuilder.append(TAB).append("<top-info>").append(topContent)
				.append("</top-info>").append(CRLF);
		xmlBuilder.append(TAB).append("<alarm-level-info>")
				.append(alarmLevelContent).append("</alarm-level-info>")
				.append(CRLF);
		xmlBuilder.append(TAB).append("<rule-description>").append("<![CDATA[")
				.append(ruleDescription).append("]]>")
				.append("</rule-description>").append(CRLF);
		// xmlBuilder.append(TAB).append("<memo>").append(memo).append("</memo>").append(CRLF);

		xmlBuilder.append("</task>").append(CRLF);

		return xmlBuilder.toString();
	}

	/**
	 * 获取子任务告警级别 取所有告警级别中最低的（AlarmLevel數字越大，告警級別越低）
	 * 
	 * @param job
	 * @return
	 */
	public short getHightAlarmLevel() {
		short level = 16;

		// 处理表达式
		List<Indicator> indicatorList = expression.getIndicatorList();
		for (Indicator indicator : indicatorList) {
			if (indicator.getAlarmLevel() < level) {
				level = indicator.getAlarmLevel();
			}
		}

		// 处理alarm-level節點 获取 表达式与任务规则中告警级别设置的两者中最大值，
		AlarmLevel alarmLevel = rule.getAlarmLevel();
		if (alarmLevel != null) {
			List<Alarm> alarmList = alarmLevel.getAlarmList();
			for (Alarm alarm : alarmList) {
				if (alarm != null) {
					if (alarm.getLevel() < level) {
						level = alarm.getLevel();
					}
				}
			}
		}

		return level;
	}

	/**
	 * 因为监控任务中基础信息和数据源信息是相同的 本方法用于解析下发监控任务时解析成多个MonitorTask,以对应数据库记录
	 */
	@Override
	public MonitorTask clone() throws CloneNotSupportedException {
		MonitorTask task = (MonitorTask) super.clone();
		return task;
	}

}
