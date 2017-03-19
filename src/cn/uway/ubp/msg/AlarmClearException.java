package cn.uway.ubp.msg;

/**
 * 告警清除异常类
 * 
 * @author Chris @ 2013-10-22
 */
public class AlarmClearException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5580822437844524358L;

	public AlarmClearException(String message) {
		super(message);
	}

	public AlarmClearException(String message, Throwable cause) {
		super(message, cause);
	}

}
