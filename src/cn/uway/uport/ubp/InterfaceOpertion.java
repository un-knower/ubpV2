package cn.uway.uport.ubp;

import javax.xml.rpc.holders.BooleanHolder;
import javax.xml.rpc.holders.IntHolder;
import javax.xml.rpc.holders.StringHolder;

/**
 * <pre>
 * ubp的接口操作。
 * 
 * @author Chris 20131017
 * </pre>
 */
public interface InterfaceOpertion {

	/**
	 * <pre>
	 * 添加监控任务
	 * 
	 * @param xmlStrTask 监控任务XML格式串
	 * @param xmlStrSecure 安全校验格式XML格式串
	 * @param msg 状态详细信息
	 * @return 状态
	 * </pre>
	 */
	public int addMonitorTask(String xmlStrTask, String xmlStrSecure, StringHolder msg);

	/**
	 * <pre>
	 * 查询指定监控任务
	 * 
	 * @param monitorTaskId 监控任务Id
	 * @param xmlStrSecure 安全校验格式XML格式串
	 * @param xmlStrResponse 响应XML格式串
	 * @param status 状态
	 * @param 状态
	 * </pre>
	 */
	public void showMonitorTaskStatusById(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status);

	/**
	 * <pre>
	 * 查询所有监控任务
	 * 
	 * @param xmlStrSecure 安全校验格式XML格式串
	 * @param xmlStrResponse 响应XML格式串
	 * @param status 状态
	 * @param 状态
	 * </pre>
	 */
	public void showOwnerMonitorTasks(String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status);

	/**
	 * <pre>
	 * 删除监控任务
	 * 
	 * @param monitorTaskId 监控任务Id
	 * @param xmlStrSecure 安全校验格式XML格式串
	 * @return 状态
	 * </pre>
	 */
	public int deleteMonitorTask(String monitorTaskId, String xmlStrSecure);

	/**
	 * <pre>
	 * 删除监控任务
	 * 仅删除正常任务，告警清除任务不删除
	 * 
	 * @param monitorTaskId 监控任务Id
	 * @param xmlStrSecure 安全校验格式XML格式串
	 * @return 状态
	 * </pre>
	 */
	public int deleteMonitorTaskWithoutAlarmClear(String monitorTaskId, String xmlStrSecure);
	
	/**
	 * <pre>
	 * 修改监控任务
	 * 
	 * @param monitorTaskId 监控任务Id
	 * @param xmlStrNewTask 监控任务XML格式串
	 * @param xmlStrSecure 安全校验格式XML格式串
	 * @param msg 状态详细信息
	 * @return 状态
	 * </pre>
	 */
	public int modifyMonitorTask(String monitorTaskId, String xmlStrNewTask, String xmlStrSecure, StringHolder msg);

	/**
	 * <pre>
	 * 设置监控任务状态
	 * @param monitorTaskId 监控任务Id
	 * @param xmlStrSecure 安全校验格式XML格式串
	 * @param oper 操作方式 1全部启用，0全部禁用，-1仅启用告警清除
	 * @return
	 * </pre>
	 */
	public int setMonitorTaskStatus(String monitorTaskId, String xmlStrSecure, Integer oper);

	/**
	 * <pre>
	 * 检查监控任务Id是否存在
	 * @param monitorTaskId 监控任务Id
	 * @param xmlStrSecure 安全校验格式XML格式串
	 * @param result 返回true表示存在，否则返回false
	 * @param msg 状态不为0（有错误发生时）的消息，正常情况为空串
	 * @return 状态
	 * </pre>
	 */
	public int existsMonitorTask(String monitorTaskId, String xmlStrSecure, BooleanHolder result, StringHolder msg);
	
	/**
	 * <pre>
	 * 获取指定监控任务的全部信息
	 * @param monitorTaskId 监控任务Id
	 * @param xmlStrSecure 安全校验格式XML格式串
	 * @param xmlStrResponse 监控任务信息
	 * @param status 状态
	 * </pre>
	 */
	public void showMonitorTask(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status);

}
