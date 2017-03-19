package cn.uway.uport.context;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.uport.dao.ProgramDefineDAO;
import cn.uway.util.entity.ProgramDefine;

/**
 * 程序定义校验器 本类缓存程序定义信息表 可在配置文件中设置缓存过期时间(自动刷新缓存的间隔时间),也可以在控制台使用手工调用指令刷新
 * 
 * @author Chris 2013年7月24日
 */
public class ProgramDefineValidator {

	/**
	 * 程序信息校验功能开关 如为false，则表示不开启程序校验。默认true
	 */
	private static boolean ENABLE = true;

	/**
	 * 程序定义缓存
	 */
	private static Set<Integer> PROGRAM_DEFINE;

	/**
	 * 定时调度线程池
	 */
	private static ScheduledExecutorService scheduleExecutor;

	/**
	 * 读写锁应用于"程序定义缓存表"被刷新替换时 非严谨的做法,其实不加锁也没有太大问题
	 */
	private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	/**
	 * 读锁
	 */
	private static ReadLock readLock = lock.readLock();

	/**
	 * 写锁
	 */
	private static WriteLock writeLock = lock.writeLock();

	/**
	 * 日志
	 */
	private static final Logger logger = LoggerFactory.getLogger(ProgramDefineValidator.class);

	/**
	 * 初始化:<br>
	 * 1、载入初始数据<br>
	 * 2、初始化调度器 <br>
	 * 3、添加调度任务
	 */
	public static void initialize() {
		logger.debug("开始初始化程序定义缓存。");

		ENABLE = Configuration.getBoolean(Configuration.SECURITY_ENABLE);
		if (!ENABLE) {
			logger.debug("程序定义校验模块开关关闭，UPORT程序定义功能未启动");
			return;
		}
		/**
		 * 预先载入程序定义信息 在这里预先载入数据考虑: <br>
		 * 1,不利用单例特性在第一次使用时再初始化,是考虑到如果数据过多,加载/解析花过多时间<br>
		 * 2,不利用调度线程池做初始化,是考虑到如果调度尚未开始,但已经有外部请求调用了,导致返回结果不准确
		 */
		try {
			loadAllProgramDefine();
		} catch (Exception e) {
			throw new RuntimeException("程序定义校验器初始化失败", e);
		}
		int delay = Configuration.getInteger(Configuration.SECURITY_PROGRAM_DEFINE_REFRESH_PERIOD);
		logger.debug("初始化程序定义缓存成功，共{}条程序定义被缓存。开始启动定时刷新任务,刷新频率={}分钟", new Object[]{PROGRAM_DEFINE.size(), delay});
		// 初始化定时调度线程池
		scheduleExecutor = Executors.newSingleThreadScheduledExecutor();
		scheduleExecutor.scheduleWithFixedDelay(new RefreshSecurityInfoTask(), 0, delay, TimeUnit.MINUTES);
	}

	/**
	 * 刷新缓存中的数据
	 * 
	 * @throws SQLException
	 */
	public static void loadAllProgramDefine() throws SQLException {
		List<ProgramDefine> pdList = ProgramDefineDAO.getInstance().loadAllProgramDefine();
		Set<Integer> tmp = new HashSet<>();
		for (ProgramDefine pd : pdList) {
			tmp.add(pd.getProgramId());
		}

		writeLock.lock();
		try {
			PROGRAM_DEFINE = tmp;
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * 校验callerId是否是已经在程序定义表中定义过<br>
	 * 
	 * @param callerId
	 *            调用者
	 * @return 是否存在
	 */
	public static boolean check(int callerId) {
		readLock.lock();
		try {
			if (!ENABLE) {
				logger.debug("程序定义校验模块开关关闭，UPORT程序定义功能未启动");
				return true;
			}
			return PROGRAM_DEFINE.contains(callerId);
		} finally {
			readLock.unlock();
		}
	}

}

/**
 * 刷新程序定义信息任务
 * 
 * @author Chris 2013年7月24日
 */
class RefreshProgramDefineInfoTask implements Runnable {

	/**
	 * 日志
	 */
	private static final Logger logger = LoggerFactory.getLogger(RefreshProgramDefineInfoTask.class);

	@Override
	public void run() {
		try {
			ProgramDefineValidator.loadAllProgramDefine();
		} catch (Exception e) {
			logger.error("刷新程序定义信息失败", e);
		}
	}

}
