package cn.uway.ubp.msg;

import java.util.HashMap;
import java.util.Map;

/**
 * 错误码
 * 
 * @author zqing @ 2013-10-9
 */
public class MessageCode {

	public static final int Code_200 = 200;

	public static final int Code_301 = 301;

	public static final int Code_302 = 302;

	static final Map<Integer, String> map = new HashMap<Integer, String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
			put(Code_200, "成功");
			put(Code_301, "加载文件失败");
			put(Code_302, "数据过滤失败");
		}
	};

	public static String getMessage(int code) {
		return map.get(code);
	}

}
