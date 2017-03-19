package cn.uway.ubp.msg;

/**
 * 规则异常类
 * 
 * @author zqing @ 2013-10-9
 */
public class RuleException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 错误码
	 */
	private int code;

	public RuleException(String message) {
		super(message);
	}

	public RuleException(Throwable e) {
		super(e);
	}

	public RuleException(String message, Throwable e) {
		super(message, e);
	}

	public RuleException(int code, String message, Throwable e) {
		this(message, e);
		this.setCode(code);
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

}
