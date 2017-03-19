package cn.uway.ubp.monitor.gc;

import java.util.Date;
import java.util.Timer;

import cn.uway.framework.util.MemCleaner;
import cn.uway.ubp.monitor.context.Configuration;

/**
 * UBP 监控模块资源回收器<br>
 * 独立线程.以小时为粒度 定时清理监控模块中的数据源和事件<br>
 * 
 * @author chenrongqiang 2013-6-1
 */
public class CollectionDepot extends Thread {

	/**
	 * 定时调度器
	 */
	protected Timer timer = new Timer();

	/**
	 * 定时任务的运行周期 最好是做成配置的形式
	 */
	private final long period;

	/**
	 * 在最小访问数据时间基础上的滑动时间
	 */
	private final int slideHours;

	/**
	 * 数据文件存储时间 单位：小时
	 */
	private final long liveHours;

	public CollectionDepot() {
		this.setName("资源回收站");
		period = Configuration.getInteger(Configuration.DATASOURCE_DEPOT_PERIOD) * 60 * 1000L;
		slideHours = Configuration.getInteger(Configuration.DATASOURCE_SLIDE_HOUR);
		liveHours = Configuration.getInteger(Configuration.DATASOURCE_LIVE_HOUR) * 3600000l;
	}

	public void startRun() {
		new CollectionDepot().start();
	}

	public void run() {
		// 调度线程1小时执行一次垃圾清理
		timer.schedule(new Cleaner(slideHours, liveHours), new Date(), period);
		startMemClear();
	}

	/**
	 * 内存回收
	 */
	private void startMemClear() {

		MemCleaner memclear = new MemCleaner();

		long memoryPeriodMills = Configuration.getInteger(Configuration.MEMEROY_GC_PERIOD) * 60 * 1000L;
		if (memoryPeriodMills > 0)
			memclear.setPeriodMills(memoryPeriodMills);

		int memoryPercent = Configuration.getInteger(Configuration.MAX_MEMEROY_USE);
		if (memoryPercent > 0)
			memclear.setThreoldPercent(memoryPercent);

		memclear.start();
	}

	public static void main(String[] args) {
		new CollectionDepot().startRun();
	}
}
