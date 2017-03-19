package cn.uway.uport.context;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 接口访问频次管理
 * 该控制方法有待讨论，改进
 * @author ChensSijiang 2013-05-23
 */
public class AccessControl {

	/**
	 * UPORT访问控制开关 false则表示UPORT不校验访问频率，默认为true
	 */
	private static boolean ENABLE = true;

	/* 存储IP访问信息，以IP为键。 */
	private static final Map<String, AccessEntry> data = new HashMap<String, AccessEntry>(64);

	/* 判断周期（毫秒）。 */
	private static long ACCESS_CONTROL_UNIT_SECONDS;

	/* 判断周期内的最大访问次数。 */
	private static int ACCESS_CONTROL_ACCESS_COUNT;

	/* 达到访问次数限制后，要求调用者休息多少秒。 */
	private static long ACCESS_CONTROL_FORBIDDEN_TIME;

	/* IP白名单，如果为null，则表示没有IP限制。 */
	private static List<String> ACCESS_CONTROL_ALLOW_IP_LIST;

	private static final Logger logger = LoggerFactory.getLogger(AccessControl.class);

	public static final void initialize() {
		logger.debug("开始初始化UPORT访问控制模块");
		ENABLE = Configuration.getBoolean(Configuration.ACCESS_CONTROL_ENABLE);
		if (!ENABLE) {
			logger.debug("UPORT访问控制开关关闭，UPort访问控制功能未启动");
			return;
		}
		ACCESS_CONTROL_UNIT_SECONDS = Configuration.getInteger(Configuration.ACCESS_CONTROL_UNIT_SECONDS) * 1000L;
		ACCESS_CONTROL_ACCESS_COUNT = Configuration.getInteger(Configuration.ACCESS_CONTROL_ACCESS_COUNT);
		ACCESS_CONTROL_FORBIDDEN_TIME = Configuration.getInteger(Configuration.ACCESS_CONTROL_FORBIDDEN_TIME) * 1000L;
		ACCESS_CONTROL_ALLOW_IP_LIST = new LinkedList<String>();
		String allowIpList = Configuration.getString(Configuration.ACCESS_CONTROL_ALLOW_IP_LIST);
		if (allowIpList != null)
			ACCESS_CONTROL_ALLOW_IP_LIST = Arrays.asList(allowIpList.split(","));
	}

	/**
	 * <pre>
	 * 检查一个IP的访问是否合法
	 * 
	 * @param ip ip地址。
	 * @throws AccessControlException 访问不合法
	 * </pre>
	 */
	public static final void check(String ip) throws AccessControlException {
		if (!ENABLE) {
			return;
		}
		
		if (StringUtils.isBlank(ip))
			throw new AccessControlException("请求者的IP地址为空");

		if (ACCESS_CONTROL_ALLOW_IP_LIST != null && !ACCESS_CONTROL_ALLOW_IP_LIST.contains("*") && !ACCESS_CONTROL_ALLOW_IP_LIST.contains(ip))
			throw new AccessControlException("请求者的IP地址未在白名单内：" + ip);

		long curr = System.currentTimeMillis();
		AccessEntry entry = null;
		synchronized (data) {
			entry = data.get(ip);
		}
		if (entry == null) {
			// 第一次访问接口
			entry = new AccessEntry(curr, 1);
			synchronized (data) {
				data.put(ip, entry);
			}
			return;
		}

		entry.count++;
		long checkPeriodStartTimeMills = entry.checkPeriodStartTime;
		
		// 访问超过了检查周期且访问次数未超过，重置
		if ((curr - checkPeriodStartTimeMills > ACCESS_CONTROL_UNIT_SECONDS) && (entry.count < ACCESS_CONTROL_ACCESS_COUNT)) {
			entry.checkPeriodStartTime = curr;
			entry.count = 1;
			entry.sleepStartTime = 0;
			return;
		} else {
			// 合法访问：未超过检查周期，且访问次数在控制值内；或超过访问次数，但在检查周期内
			if ((entry.count < ACCESS_CONTROL_ACCESS_COUNT)
					|| ((entry.count >= ACCESS_CONTROL_ACCESS_COUNT) && (curr - checkPeriodStartTimeMills < ACCESS_CONTROL_UNIT_SECONDS))) {
				return;
			}
		}
		
		// 上次就是已超过限制的状态了，但休眠时间已到，将此IP的信息重置，重新计时、计数
		if (entry.sleepStartTime > 0 && curr - entry.sleepStartTime >= ACCESS_CONTROL_FORBIDDEN_TIME) {
			entry.reset();
			return;
		}
		
		// 计算此IP还需要休眠多久
		long leftSecond = (entry.sleepStartTime == 0 ? ACCESS_CONTROL_FORBIDDEN_TIME : ACCESS_CONTROL_FORBIDDEN_TIME - (curr - entry.sleepStartTime));
		if (entry.sleepStartTime == 0)
			entry.sleepStartTime = curr;
		
		throw new AccessControlException("此IP访问频次过高，" + (ACCESS_CONTROL_UNIT_SECONDS/1000) + "秒内访问了" + entry.count + "次，已超过" + ACCESS_CONTROL_ACCESS_COUNT
				+ "次的限制，请在" + (leftSecond/1000.0f) + "秒之后再进行访问");
	}
	
	public static void main(String[] args) throws Exception {
		Configuration.initialize(null);
		AccessControl.initialize();
		
		long time = System.currentTimeMillis();
		for (int i=1; i<=1000000; i++) {
			try {
				check("127.0.0.1");
				System.out.println("check ok\t" + i);
			} catch (Exception e) {
				System.err.println("访问异常：" + e.getMessage());
			}
			
			Thread.sleep(10L);
		}
		System.out.println("耗时：" + (System.currentTimeMillis() - time) + "ms");
	}
	
}

/**
 * IP访问信息。
 * 
 * @author ChensSijiang 2013-05-23
 */
class AccessEntry {

	/**
	 * 一个检查周期的开始时间，系统毫秒数。
	 */
	long checkPeriodStartTime;

	/**
	 * 当IP超过访问次数限制后，要休眠多少毫秒。
	 */
	long sleepStartTime;

	/**
	 * 一个检查周期的访问次数，计数。
	 */
	int count;

	public AccessEntry(long checkPeriodStartTime, int count) {
		super();
		this.checkPeriodStartTime = checkPeriodStartTime;
		this.count = count;
	}
	
	public void reset() {
		checkPeriodStartTime = System.currentTimeMillis();
		count = 0;
		sleepStartTime = 0L;
	}

	@Override
	public String toString() {
		return "AccessEntry [checkPeriodStartTime=" + checkPeriodStartTime + ", sleepStartTime=" + sleepStartTime + ", count=" + count + "]";
	}

}