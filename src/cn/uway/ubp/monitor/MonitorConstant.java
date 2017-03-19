package cn.uway.ubp.monitor;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * 常量及全局变量定义类
 * 
 * @author zhouq Date :2013-5-29
 */
public class MonitorConstant {

	/**
	 * 序列化文件的键值分割符，对应文件路径目录结构
	 */
	// public static final String SERIALIZE_SPLIT = ;

	/**
	 * 数据的键值分割符，
	 */
	public static final String KEY_SPLIT = "__";

	/**
	 * 数据源运行线程池
	 */
	public static int sourcePoolSize = 10;

	/**
	 * 严重
	 */
	public final static short SERIOUS = 1;

	/**
	 * 重要
	 */
	public final static short MAJOR = 2;

	/**
	 * 普通
	 */
	public final static short GENERAL = 4;

	/**
	 * 异常
	 */
	public final static short UNUSUAL = 8;

	/**
	 * 预警
	 */
	public final static short NOTICE = 16;
	
	/**
	 * 驳回告警
	 */
	public final static short REJECT = 9999;

	public final static Map<Short, String> levelMap = new HashMap<>();

	static {
		levelMap.put(SERIOUS, "严重告警");
		levelMap.put(MAJOR, "重要告警");
		levelMap.put(GENERAL, "普通告警");
		levelMap.put(UNUSUAL, "异常告警");
		levelMap.put(NOTICE, "预警告警");
		levelMap.put(REJECT, "驳回告警");
	}

}
