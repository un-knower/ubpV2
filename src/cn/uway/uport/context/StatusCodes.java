package cn.uway.uport.context;

/**
 * Webservice请求结果状态码。
 * 
 * @author ChensSijiang 2013-5-21
 */
public interface StatusCodes {

	/**
	 * 成功。
	 */
	public static final int SUCC = 0;

	/**
	 * 安全验证失败。
	 */
	public static final int SECURE_FAIL = 1;

	/**
	 * 请求内容解析失败。
	 */
	public static final int REQ_PARSE_FAIL = 2;

	/**
	 * 内部数据库错误。
	 */
	public static final int INTERNAL_DATABASE_FAIL = 3;

	/**
	 * 不存在的编号。
	 */
	public static final int NOT_EXISTS_ID = 4;

	/**
	 * 未能获取到请求者的IP地址，访问不被允许。
	 */
	public static final int IP_ERROR = 5;

	/**
	 * 请求者的IP不在白名单内，不允许访问。
	 */
	public static final int IP_NOT_ALLOW = 6;

	/**
	 * 请求者的访问次数太频繁。
	 */
	public static final int IP_MAX_ACCESS = 7;

	/**
	 * 空闲状态,没有无运行状态 2013.7.9添加
	 */
	public static final int MONITOR_TASK_IDLE = 8;

	/**
	 * 无效状态,运行运行时出错 2013.7.18添加
	 */
	public static final int MONITOR_TASK_INVALID = 9;

}
