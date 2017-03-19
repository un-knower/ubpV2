package cn.uway.uport.ubp;

import java.sql.SQLException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.xml.rpc.holders.BooleanHolder;
import javax.xml.rpc.holders.IntHolder;
import javax.xml.rpc.holders.StringHolder;

import org.apache.axis.MessageContext;
import org.apache.axis.transport.http.HTTPConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXParseException;

import cn.uway.uport.context.AccessControl;
import cn.uway.uport.context.AccessControlException;
import cn.uway.uport.context.Configuration;
import cn.uway.uport.context.ProgramDefineValidator;
import cn.uway.uport.context.SecurityValidator;
import cn.uway.uport.context.StatusCodes;
import cn.uway.uport.dao.AccessLogDAO;
import cn.uway.uport.dao.MonitorTaskDAO;
import cn.uway.uport.dao.MonitorTaskStatusDAO;
import cn.uway.util.Resolver;
import cn.uway.util.entity.MonitorTask;
import cn.uway.util.entity.MonitorTaskStatus;
import cn.uway.util.entity.SecureInfo;

/**
 * 接口操作实现
 * 
 * @author Chris 2013年7月10日
 */
public final class InterfaceOpertionImpl implements InterfaceOpertion {

	/**
	 * 日志
	 */
	private static final Logger logger = LoggerFactory.getLogger(InterfaceOpertionImpl.class);

	private static final String REQ_CONTENT_SEP = "  ***  ";

	/**
	 * xml文件头信息
	 */
	private static final String XML_HEAD = "<?xml version=\"1.0\" encoding=\"utf-8\"?>";

	/**
	 * 换行符
	 */
	private static final String CRLF = "\r\n";

	/**
	 * 单例对象
	 */
	private static InterfaceOpertion instance = new InterfaceOpertionImpl();

	/**
	 * 访问日志表DAO类
	 */
	private AccessLogDAO accessLogDAO;

	/**
	 * 监控任务DAO类
	 */
	private MonitorTaskDAO monitorTaskDAO;

	/**
	 * 监控任务状态表DAO类
	 */
	private MonitorTaskStatusDAO monitorTaskStatusDAO;

	/**
	 * 私有构造
	 */
	private InterfaceOpertionImpl() {
		super();
		accessLogDAO = AccessLogDAO.getInstance();
		monitorTaskDAO = MonitorTaskDAO.getInstance();
		monitorTaskStatusDAO = MonitorTaskStatusDAO.getInstance();
	}

	/**
	 * <pre>
	 * 获取当前类实例,单例工厂方法
	 * 
	 * @return InterfaceOpertion
	 * </pre>
	 */
	public static InterfaceOpertion getInstance() {
		return instance;
	}

	@SuppressWarnings("deprecation")
	private final SecureInfo validate(String securityXml, IntHolder status, String method) {
		// TODO 请求频次限制,下一个版本具体考虑(-)
		try {
			AccessControl.check(getReqestAddr());
		} catch (AccessControlException e) {
			logger.warn("访问不被允许", e);
			int stat = e.getStatus();
			switch (stat) {
				case AccessControlException.IP_ERROR :
					status.value = StatusCodes.IP_ERROR;
					break;
				case AccessControlException.IP_NOT_ALLOW :
					status.value = StatusCodes.IP_NOT_ALLOW;
					break;
				case AccessControlException.MAX_ACCESS_REACHED :
					status.value = StatusCodes.IP_MAX_ACCESS;
					break;
				default :
					break;
			}
			// 添加访问记录
			accessLogDAO.add(null, -1, getReqestAddr(), securityXml, method, e.getMessage(), status.value);
			return null;
		}
		// 安全信息校验
		SecureInfo secureInfo = null;
		try {
			int calledId = Configuration.getInteger(Configuration.SECURITY_PROGRAM_ID);
			secureInfo = Resolver.resolveSecureInfo(securityXml, calledId);
			// 检查是否合法的callerId
			if (!ProgramDefineValidator.check(secureInfo.getCallerId()))
				throw new Exception("非法调用,caller未定义");
			// 校验是否合法的调用者
			if (!SecurityValidator.check(secureInfo))
				throw new Exception("验证失败。");
			return secureInfo;
		} catch (Exception e) {
			logger.error("安全信息验证失败：{}", securityXml, e);
			status.value = StatusCodes.SECURE_FAIL;
			accessLogDAO.add(null, (secureInfo != null ? secureInfo.getCallerId() : -1), getReqestAddr(), securityXml, method, e.getMessage(),
					StatusCodes.SECURE_FAIL);
			return null;
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public int addMonitorTask(String taskXml, String securityXml, StringHolder msg) {
		IntHolder status = new IntHolder();
		SecureInfo secureInfo = validate(securityXml, status, "public int addMonitorTask(String taskXml, String securityXml, StringHolder msg)");
		if (secureInfo == null)
			return status.value;
		List<MonitorTask> taskList = null;
		try {
			taskList = Resolver.resolveMonitorTask(taskXml);
		} catch (SAXParseException e) {
			String errMsg = "校验任务信息失败！行：" + e.getLineNumber() + "，列：" + e.getColumnNumber() + "，错误信息：" + e.getMessage();

			logger.error(errMsg, e);
			msg.value = errMsg;
			accessLogDAO.add(null, secureInfo.getCallerId(), getReqestAddr(), taskXml,
					"public int addMonitorTask(String taskXml, String securityXml, StringHolder msg)", errMsg, StatusCodes.REQ_PARSE_FAIL);

			return StatusCodes.REQ_PARSE_FAIL;
		} catch (Exception e) {
			logger.error("解析任务信息失败", e);

			msg.value = "解析任务信息失败:" + e.getMessage();

			accessLogDAO.add(null, secureInfo.getCallerId(), getReqestAddr(), taskXml,
					"public int addMonitorTask(String taskXml, String securityXml, StringHolder msg)", msg.value, StatusCodes.REQ_PARSE_FAIL);

			return StatusCodes.REQ_PARSE_FAIL;
		}

		int returnValue;
		try {
			monitorTaskDAO.add(taskList);

			msg.value = "success"; // 本字段不能明确表示(受电信3期项目修改影响20130829)
			returnValue = StatusCodes.SUCC;

			accessLogDAO.add(taskList.get(0).getMonitorTaskId(), secureInfo.getCallerId(), getReqestAddr(), taskXml,
					"public int addMonitorTask(String taskXml, String securityXml, StringHolder msg)", "taskId=0", StatusCodes.SUCC);
		} catch (Exception e) {
			logger.error("添加任务信息失败", e);

			msg.value = "添加任务信息失败:" + e.getMessage();
			returnValue = StatusCodes.INTERNAL_DATABASE_FAIL;

			accessLogDAO.add(taskList.get(0).getMonitorTaskId(), secureInfo.getCallerId(), getReqestAddr(), taskXml,
					"public int addMonitorTask(String taskXml, String securityXml, StringHolder msg)", e.getMessage(),
					StatusCodes.INTERNAL_DATABASE_FAIL);
		}

		return returnValue;
	}

	@SuppressWarnings("deprecation")
	@Override
	public int modifyMonitorTask(String monitorTaskId, String xmlStrNewTask, String xmlStrSecure, StringHolder msg) {
		// 安全校验
		IntHolder status = new IntHolder();
		SecureInfo secureInfo = validate(xmlStrSecure, status,
				"public int modifyMonitorTask(String monitorTaskId, String xmlStrNewTask, String xmlStrSecure, StringHolder msg)");
		if (secureInfo == null)
			return status.value;

		// 任务解析
		List<MonitorTask> taskList = null;
		try {
			taskList = Resolver.resolveMonitorTask(xmlStrNewTask);
		} catch (SAXParseException e) {
			String errMsg = "校验任务信息失败！行：" + e.getLineNumber() + "，列：" + e.getColumnNumber() + "，错误信息：" + e.getMessage();

			logger.error(errMsg, e);
			msg.value = errMsg;
			accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrNewTask
					+ REQ_CONTENT_SEP + xmlStrSecure,
					"public int modifyMonitorTask(String monitorTaskId, String xmlStrNewTask, String xmlStrSecure, StringHolder msg)", errMsg,
					StatusCodes.REQ_PARSE_FAIL);

			return StatusCodes.REQ_PARSE_FAIL;
		} catch (Exception e) {
			logger.error("解析任务信息失败", e);

			msg.value = "解析任务信息失败:" + e.getMessage();

			accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrNewTask
					+ REQ_CONTENT_SEP + xmlStrSecure,
					"public int modifyMonitorTask(String monitorTaskId, String xmlStrNewTask, String xmlStrSecure, StringHolder msg)",
					e.getMessage(), StatusCodes.REQ_PARSE_FAIL);

			return StatusCodes.REQ_PARSE_FAIL;
		}

		// 参数校验
		if (StringUtils.isBlank(monitorTaskId)) {
			accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
					"public int modifyMonitorTask(String monitorTaskId, String xmlStrNewTask, String xmlStrSecure, StringHolder msg)", "",
					StatusCodes.REQ_PARSE_FAIL);

			msg.value = "MonitorTaskId为空";

			return StatusCodes.REQ_PARSE_FAIL;
		}

		int returnValue;
		int callerId = secureInfo.getCallerId();
		try {
			monitorTaskDAO.modify(taskList, monitorTaskId, callerId);

			accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrNewTask
					+ REQ_CONTENT_SEP + xmlStrSecure,
					"public int modifyMonitorTask(String monitorTaskId, String xmlStrNewTask, String xmlStrSecure, StringHolder msg)", "true",
					StatusCodes.SUCC);

			msg.value = "success";
			returnValue = StatusCodes.SUCC;
		} catch (SQLException e) {
			logger.error("修改任务失败", e);

			msg.value = "修改任务失败:" + e.getMessage();
			returnValue = StatusCodes.INTERNAL_DATABASE_FAIL;

			accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrNewTask
					+ REQ_CONTENT_SEP + xmlStrSecure,
					"public int modifyMonitorTask(String monitorTaskId, String xmlStrNewTask, String xmlStrSecure, StringHolder msg)",
					e.getMessage(), StatusCodes.INTERNAL_DATABASE_FAIL);
		} catch (Exception e) {
			logger.error("修改任务失败，监控任务不存在：{}", monitorTaskId, e);

			msg.value = "修改任务失败，监控任务不存在:" + monitorTaskId;
			returnValue = StatusCodes.NOT_EXISTS_ID;

			accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrNewTask
					+ REQ_CONTENT_SEP + xmlStrSecure,
					"public int modifyMonitorTask(String monitorTaskId, String xmlStrNewTask, String xmlStrSecure, StringHolder msg)",
					e.getMessage(), StatusCodes.NOT_EXISTS_ID);
		}

		return returnValue;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void showMonitorTaskStatusById(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status) {
		SecureInfo secureInfo = validate(xmlStrSecure, status,
				"public void queryMonitTaskStatus(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)");
		if (secureInfo == null)
			return;

		if (StringUtils.isBlank(monitorTaskId)) {
			status.value = StatusCodes.REQ_PARSE_FAIL;

			accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
					"public void queryMonitTaskStatus(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)", "",
					StatusCodes.REQ_PARSE_FAIL);

			return;
		}

		int callerId = secureInfo.getCallerId();

		try {
			boolean exists = monitorTaskDAO.exists(monitorTaskId, callerId);
			if (!exists) {
				// 任务不存在
				status.value = StatusCodes.NOT_EXISTS_ID;

				accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
						"public void queryMonitTaskStatus(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)",
						"", StatusCodes.NOT_EXISTS_ID);

				return;
			}

			List<MonitorTaskStatus> statusAry = monitorTaskStatusDAO.getMonitorTaskStatusById(callerId, monitorTaskId);
			if (statusAry == null) {
				// 没有监控记录
				status.value = StatusCodes.MONITOR_TASK_IDLE;

				accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
						"public void queryMonitTaskStatus(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)",
						xmlStrResponse.value, StatusCodes.MONITOR_TASK_IDLE);

				return;
			}

			StringBuilder xmlBuilder = new StringBuilder();
			xmlBuilder.append(XML_HEAD).append(CRLF);
			xmlBuilder.append("<tasks>").append(CRLF);

			for (MonitorTaskStatus mts : statusAry) {
				xmlBuilder.append(mts.toXML()).append(CRLF);
			}

			xmlBuilder.append("</tasks>").append(CRLF);

			xmlStrResponse.value = xmlBuilder.toString();
			status.value = StatusCodes.SUCC;

			accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
					"public void queryMonitTaskStatus(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)",
					xmlStrResponse.value, StatusCodes.SUCC);
		} catch (Exception e) {
			logger.error("查询任务状态失败：{}", monitorTaskId, e);

			status.value = StatusCodes.INTERNAL_DATABASE_FAIL;

			accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
					"public void queryMonitTaskStatus(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)",
					e.getMessage(), StatusCodes.INTERNAL_DATABASE_FAIL);
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public void showOwnerMonitorTasks(String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status) {
		SecureInfo secureInfo = validate(xmlStrSecure, status,
				"public void showOwnerMonitTasks(String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)");
		if (secureInfo == null)
			return;

		int callerId = secureInfo.getCallerId();

		try {
			List<MonitorTaskStatus> statusAry = monitorTaskStatusDAO.getAllMonitorTaskStatus(callerId);

			StringBuilder xmlBuilder = new StringBuilder();
			xmlBuilder.append(XML_HEAD).append(CRLF);
			xmlBuilder.append("<tasks>").append(CRLF);

			if (statusAry != null) {
				for (MonitorTaskStatus mts : statusAry) {
					xmlBuilder.append(mts.toXML()).append(CRLF);
				}
			}

			xmlBuilder.append("</tasks>").append(CRLF);

			xmlStrResponse.value = xmlBuilder.toString();
			status.value = StatusCodes.SUCC;

			accessLogDAO.add(null, secureInfo.getCallerId(), getReqestAddr(), xmlStrSecure,
					"public void showOwnerMonitTasks(String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)", xmlStrResponse.value,
					status.value);
		} catch (Exception e) {
			logger.error("查询任务状态失败：{}", callerId, e);

			status.value = StatusCodes.INTERNAL_DATABASE_FAIL;

			accessLogDAO.add(null, secureInfo.getCallerId(), getReqestAddr(), xmlStrSecure,
					"public void showOwnerMonitTasks(String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)", e.getMessage(),
					status.value);
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public int deleteMonitorTask(String monitorTaskId, String xmlStrSecure) {
		IntHolder status = new IntHolder();
		SecureInfo secureInfo = validate(xmlStrSecure, status, "public int deleteMonitTask(String monitorTaskId, String xmlStrSecure)");
		if (secureInfo == null)
			return status.value;

		int callerId = secureInfo.getCallerId();

		try {
			if (StringUtils.isBlank(monitorTaskId))
				throw new IllegalArgumentException("非法参数,monitorTaskId为空");

			boolean result = monitorTaskDAO.del(monitorTaskId, callerId);

			if (result) {
				accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
						"public int deleteMonitTask(String monitorTaskId, String xmlStrSecure)", "true", StatusCodes.SUCC);

				return StatusCodes.SUCC;
			} else {
				accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
						"public int deleteMonitTask(String monitorTaskId, String xmlStrSecure)", "false", StatusCodes.NOT_EXISTS_ID);

				return StatusCodes.NOT_EXISTS_ID;
			}
		} catch (Exception e) {
			logger.error("删除任务失败：{}", monitorTaskId, e);

			accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
					"public int deleteMonitTask(String monitorTaskId, String xmlStrSecure)", e.getMessage(), StatusCodes.INTERNAL_DATABASE_FAIL);

			return StatusCodes.INTERNAL_DATABASE_FAIL;
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public int deleteMonitorTaskWithoutAlarmClear(String monitorTaskId, String xmlStrSecure) {
		IntHolder status = new IntHolder();
		SecureInfo secureInfo = validate(xmlStrSecure, status, "public int deleteMonitTaskWithoutAlarmClear(String monitorTaskId, String xmlStrSecure)");
		if (secureInfo == null)
			return status.value;

		int callerId = secureInfo.getCallerId();

		try {
			if (StringUtils.isBlank(monitorTaskId))
				throw new IllegalArgumentException("非法参数,monitorTaskId为空");

			boolean result = monitorTaskDAO.delWithoutAlarmClear(monitorTaskId, callerId);

			if (result) {
				accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
						"public int deleteMonitTaskWithoutAlarmClear(String monitorTaskId, String xmlStrSecure)", "true", StatusCodes.SUCC);

				return StatusCodes.SUCC;
			} else {
				accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
						"public int deleteMonitTaskWithoutAlarmClear(String monitorTaskId, String xmlStrSecure)", "false", StatusCodes.NOT_EXISTS_ID);

				return StatusCodes.NOT_EXISTS_ID;
			}
		} catch (Exception e) {
			logger.error("删除任务失败：{}", monitorTaskId, e);

			accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
					"public int deleteMonitTaskWithoutAlarmClear(String monitorTaskId, String xmlStrSecure)", e.getMessage(), StatusCodes.INTERNAL_DATABASE_FAIL);

			return StatusCodes.INTERNAL_DATABASE_FAIL;
		}
	}
	
	@SuppressWarnings("deprecation")
	@Override
	public int setMonitorTaskStatus(String monitorTaskId, String xmlStrSecure, Integer oper) {
		IntHolder status = new IntHolder();
		SecureInfo secureInfo = validate(xmlStrSecure, status,
				"public int setMonitorTaskStatus(String monitorTaskId, String xmlStrSecure, Integer oper)");
		if (secureInfo == null)
			return status.value;

		int callerId = secureInfo.getCallerId();

		try {
			if (StringUtils.isBlank(monitorTaskId))
				throw new IllegalArgumentException("非法参数,monitorTaskId为空");

			boolean result = monitorTaskDAO.setUsedStatus(monitorTaskId, callerId, oper);

			if (result) {
				accessLogDAO.add(monitorTaskId, callerId, getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
						"public int setMonitorTaskStatus(String monitorTaskId, String xmlStrSecure, Integer oper)", "true", StatusCodes.SUCC);

				return StatusCodes.SUCC;
			} else {
				accessLogDAO.add(monitorTaskId, callerId, getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
						"public int setMonitorTaskStatus(String monitorTaskId, String xmlStrSecure, Integer oper)", "false",
						StatusCodes.NOT_EXISTS_ID);

				return StatusCodes.NOT_EXISTS_ID;
			}
		} catch (Exception e) {
			logger.error("暂停任务失败：{}", monitorTaskId, e);

			accessLogDAO.add(monitorTaskId, callerId, getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
					"public int setMonitorTaskStatus(String monitorTaskId, String xmlStrSecure, Integer oper)", e.getMessage(),
					StatusCodes.INTERNAL_DATABASE_FAIL);

			return StatusCodes.INTERNAL_DATABASE_FAIL;
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public int existsMonitorTask(String monitorTaskId, String xmlStrSecure, BooleanHolder result, StringHolder msg) {
		IntHolder status = new IntHolder();
		SecureInfo secureInfo = validate(xmlStrSecure, status,
				"public int existsMonitorTask(String monitorTaskId, String xmlStrSecure, BooleanHolder result, StringHolder msg)");
		if (secureInfo == null)
			return status.value;

		if (StringUtils.isBlank(monitorTaskId))
			return StatusCodes.NOT_EXISTS_ID;

		int callerId = secureInfo.getCallerId();

		int returnValue = StatusCodes.MONITOR_TASK_INVALID;

		try {
			result.value = monitorTaskDAO.exists(monitorTaskId, callerId);
			returnValue = StatusCodes.SUCC;

			if (result.value) {
				accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
						"public int existsMonitorTask(String monitorTaskId, String xmlStrSecure, BooleanHolder result, StringHolder msg)", "true",
						StatusCodes.SUCC);
			} else {
				accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
						"public int existsMonitorTask(String monitorTaskId, String xmlStrSecure, BooleanHolder result, StringHolder msg)", "false",
						StatusCodes.NOT_EXISTS_ID);
			}
		} catch (Exception e) {
			logger.error("查找任务失败：{}", monitorTaskId, e);

			msg.value = e.getMessage();

			accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
					"public int existsMonitorTask(String monitorTaskId, String xmlStrSecure, BooleanHolder result, StringHolder msg)",
					e.getMessage(), StatusCodes.INTERNAL_DATABASE_FAIL);
		}

		return returnValue;
	}
	
	@SuppressWarnings("deprecation")
	@Override
	public void showMonitorTask(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status) {
		SecureInfo secureInfo = validate(xmlStrSecure, status,
				"public void showMonitorTask(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)");
		if (secureInfo == null)
			return;

		if (StringUtils.isBlank(monitorTaskId)) {
			status.value = StatusCodes.REQ_PARSE_FAIL;

			accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
					"public void showMonitorTask(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)", "",
					StatusCodes.REQ_PARSE_FAIL);

			return;
		}

		int callerId = secureInfo.getCallerId();

		try {
			boolean exists = monitorTaskDAO.exists(monitorTaskId, callerId);
			if (!exists) {
				// 任务不存在
				status.value = StatusCodes.NOT_EXISTS_ID;

				accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
						"public void showMonitorTask(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)",
						"", StatusCodes.NOT_EXISTS_ID);

				return;
			}
			
			StringBuilder xmlBuilder = new StringBuilder();
			xmlBuilder.append(XML_HEAD).append(CRLF);
			xmlBuilder.append("<tasks>").append(CRLF);
			
			List<MonitorTask> monitorTaskAry = monitorTaskDAO.getMonitorTasks(monitorTaskId);
			for (MonitorTask mt : monitorTaskAry) {
				xmlBuilder.append(mt.toXML()).append(CRLF);
			}

			xmlBuilder.append("</tasks>").append(CRLF);

			xmlStrResponse.value = xmlBuilder.toString();
			status.value = StatusCodes.SUCC;
		} catch (Exception e) {
			logger.error("查询监控任务运行状态记录失败", e);
			
			status.value = StatusCodes.INTERNAL_DATABASE_FAIL;

			accessLogDAO.add(monitorTaskId, secureInfo.getCallerId(), getReqestAddr(), monitorTaskId + REQ_CONTENT_SEP + xmlStrSecure,
					"public void showMonitorTask(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)",
					e.getMessage(), StatusCodes.INTERNAL_DATABASE_FAIL);
		}
	}

	/**
	 * 获取请求来源IP
	 * 
	 * @return 请求来源IP
	 */
	private static final String getReqestAddr() {
		MessageContext ctx = MessageContext.getCurrentContext();
		HttpServletRequest req = (HttpServletRequest) ctx.getProperty(HTTPConstants.MC_HTTP_SERVLETREQUEST);

		return req.getRemoteAddr();
	}

}
