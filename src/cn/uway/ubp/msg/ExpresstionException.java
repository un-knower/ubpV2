package cn.uway.ubp.msg;

/**
 * 表达式异常类
 * 
 * @author zqing @ 2013-10-9
 */
public class ExpresstionException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 错误码
	 */
	private int code;

	public ExpresstionException(String message) {
		super(message);
	}

	public ExpresstionException(Throwable e) {
		super(e);
	}

	public ExpresstionException(String message, Throwable e) {
		super(message, e);
	}

	public ExpresstionException(int code, String message, Throwable e) {
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
