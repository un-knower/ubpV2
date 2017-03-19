package cn.uway.uport.context;

/**
 * IP访问不合法的异常。
 * 
 * @author ChensSijiang 2013-05-23
 */
public class AccessControlException extends Exception {

	private static final long serialVersionUID = 8359642033534886322L;

	/** 已达最大访问次数限制。 */
	public static final int MAX_ACCESS_REACHED = 5;

	/** IP不在白名单。 */
	public static final int IP_NOT_ALLOW = 6;

	/** IP地址错误。 */
	public static final int IP_ERROR = 7;

	// 原因值。
	private int status;

	public AccessControlException() {
		super();
	}

	public AccessControlException(String message, Throwable cause, int status) {
		super(message, cause);
		this.status = status;
	}

	public AccessControlException(String message) {
		super(message);
	}

	public AccessControlException(Throwable cause) {
		super(cause);
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

}
