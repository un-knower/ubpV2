package cn.uway.ubp.msg;

/**
 * 数据过滤异常类
 * 
 * @author zqing @ 2013-10-9
 */
public class FilterException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 错误码
	 */
	private int code;

	public FilterException(String message) {
		super(message);
	}

	public FilterException(Throwable e) {
		super(e);
	}

	public FilterException(String message, Throwable e) {
		super(message, e);
	}

	public FilterException(int code, String message, Throwable e) {
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
