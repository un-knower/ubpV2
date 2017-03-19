package cn.uway.uport.context;

import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.uport.dao.SecurityDAO;
import cn.uway.util.entity.SecureInfo;

/**
 * <pre>
 * 安全校验器 在Mybatis层可以实现透明缓存,但无法手工灵活控制缓存刷新,以及根据配置实现缓存自动刷新,正是这个原因跟本系统设计相冲突,因此放弃
 * 定时从UWAY_INTERFACE_SECURITY表同步数据
 * @author Chris 2013年7月12日
 * </pre>
 */
public class SecurityValidator {

	/**
	 * 内存中缓存的安全校验信息
	 */
	private static Set<String> SECUTIRY_INFO;

	/**
	 * 定时调度线程池
	 */
	private static ScheduledExecutorService scheduleExecutor;

	/**
	 * 读写锁应用于"安全信息缓存表SECUTIRY_INFO"被刷新替换
	 */
	private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	/**
	 * 程序安全信息校验开关，如果ENABLE为false，则表示UPORT不执行安全校验，默认为true
	 */
	private static boolean ENABLE = true;

	/**
	 * 读锁
	 */
	private static ReadLock readLock = lock.readLock();

	/**
	 * 写锁
	 */
	private static WriteLock writeLock = lock.writeLock();

	/**
	 * 安全信息分隔符
	 */
	private static final String INTERVAL = "_";

	/**
	 * 日志
	 */
	private static final Logger logger = LoggerFactory.getLogger(SecurityValidator.class);

	/**
	 * 初始化: <br>
	 * 1、载入初始数据 <br>
	 * 2、初始化调度器 <br>
	 * 3、添加调度任务
	 */
	public static void initialize() {
		logger.debug("开始初始化安全校验模块");
		ENABLE = Configuration.getBoolean(Configuration.SECURITY_ENABLE);
		if (!ENABLE) {
			logger.debug("安全校验模块开关关闭，UPORT安全校验功能未启动");
			return;
		}
		/**
		 * 预先载入安全校验信息 在这里预先载入数据考虑: 1,不利用单例特性在第一次使用时再初始化,是考虑到如果数据过多,加载/解析花过多时间
		 * 2,不利用调度线程池做初始化,是考虑到如果调度尚未开始,但已经有外部请求调用了,导致返回结果不准确
		 */
		try {
			loadSecurityInfo();
		} catch (Exception e) {
			throw new RuntimeException("安全校验器初始化失败", e);
		}
		logger.debug("安全校验模块初始化完成，共加载{}条安全信息", SECUTIRY_INFO.size());
		// 初始化定时调度线程池
		scheduleExecutor = Executors.newSingleThreadScheduledExecutor();
		int delay = Configuration.getInteger(Configuration.SECURITY_INFO_REFRESH_PERIOD);
		scheduleExecutor.scheduleWithFixedDelay(new RefreshSecurityInfoTask(), 0, delay, TimeUnit.MINUTES);
		logger.debug("安全信息校验定时刷新调度线程启动,刷新频率={}分钟", delay);
	}

	/**
	 * 刷新缓存的安全校验信息
	 * 
	 * @throws SQLException
	 */
	public static void loadSecurityInfo() throws SQLException {
		writeLock.lock();
		try {
			if (!ENABLE) {
				logger.debug("安全校验模块开关关闭，UPORT安全校验功能未启动");
				return;
			}
			Set<String> securityInfoSet = SecurityDAO.getInstance().loadSecurityInfo();
			SECUTIRY_INFO = securityInfoSet;
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * 校验请求信息<br>
	 * 
	 * @param callerId
	 *            调用者ID
	 * @param calledId
	 *            被调用者ID
	 * @param username
	 *            用户名
	 * @param password
	 *            密码
	 * @return boolean 是否合法的调用者
	 */
	public static boolean check(int callerId, int calledId, String username, String password) {
		readLock.lock();
		try {
			// 如果程序开关关闭，则校验直接通过
			if (!ENABLE)
				return true;
			String key = createKey(callerId, calledId, username, password);
			return SECUTIRY_INFO.contains(key);
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * 校验请求信息<br>
	 * 必须callerid,calledId,userName,password都一直才为合法的用户<br>
	 * 
	 * @param secureInfo
	 * @return boolean 是否合法的调用者
	 */
	public static boolean check(SecureInfo secureInfo) {
		readLock.lock();
		try {
			// 如果程序开关关闭，则校验直接通过
			if (!ENABLE)
				return true;
			String key = createKey(secureInfo);
			return SECUTIRY_INFO.contains(key);
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * 产生安全信息签名 每个字段间用间隔符分开
	 * 
	 * @param secureInfo
	 *            安全校验信息实体类
	 * @return 拼接后的安全校验信息
	 */
	private static String createKey(SecureInfo secureInfo) {
		return createKey(secureInfo.getCallerId(), secureInfo.getCalledId(), secureInfo.getUsername(), secureInfo.getPassword());
	}

	/**
	 * 产生安全信息签名 每个字段间用间隔符分开
	 * 
	 * @param callerId
	 *            调用者Id
	 * @param calledId
	 *            被调用者Id
	 * @param username
	 *            用户名
	 * @param password
	 *            密码
	 * @return 拼接后的安全校验信息
	 */
	public static String createKey(int callerId, int calledId, String username, String password) {
		StringBuilder key = new StringBuilder();
		key.append(callerId).append(INTERVAL);
		key.append(calledId).append(INTERVAL);
		key.append(username).append(INTERVAL);
		key.append(password);
		return key.toString();
	}

}

/**
 * 刷新安全校验信息任务
 * 
 * @author Chris 2013年7月12日
 */
class RefreshSecurityInfoTask implements Runnable {

	/**
	 * 日志
	 */
	private static final Logger logger = LoggerFactory.getLogger(RefreshSecurityInfoTask.class);

	@Override
	public void run() {
		try {
			SecurityValidator.loadSecurityInfo();
		} catch (Exception e) {
			logger.error("刷新安全校验信息失败", e);
		}
	}

}
