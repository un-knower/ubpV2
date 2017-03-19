package cn.uway.ubp.monitor.data;

import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.util.thread.ExecutorThreadPool;
import cn.uway.ubp.monitor.data.future.LoadFuture;
import cn.uway.util.entity.DataSource;

/**
 * 数据源加载后续
 * 主要用于检测加载超时的情况 以及从加载表中删除
 * @author chris
 * @ 2014年3月12日
 */
public class CompletionProcessor implements Runnable {
	/**
	 * 当前类线程对象
	 */
	private Thread myThread;
	
	/**
	 * 数据源加载线程池
	 */
	private ExecutorThreadPool<LoadFuture> loaderThreadPool;
	
	/**
	 * 正在加载的数据源
	 */
	private ConcurrentMap<Long, DataSource> loadingMap;
	
	private static final Logger logger = LoggerFactory.getLogger(CompletionProcessor.class);

	public CompletionProcessor(ExecutorThreadPool<LoadFuture> loaderThreadPool, ConcurrentMap<Long, DataSource> loadingMap) {
		this.loaderThreadPool = loaderThreadPool;
		this.loadingMap = loadingMap;
	}
	
	public void startup() {
		myThread = new Thread(this);
		myThread.start();
	}
	
	public void shutdown() {
		myThread.interrupt();
	}

	@Override
	public void run() {
		Thread.currentThread().setName("数据源加载后续");
		
		logger.debug("数据源加载后续线程运行开始");
		
		while (true) {
			try {
				LoadFuture future = loaderThreadPool.take();
				if (future == null) {
					logger.warn("数据源加载超时！");
					continue;
				}

				long dataSourceId = future.getDataSourceId();
				
				// 移除已经被处理的数据源，让数据源下次被加载
				loadingMap.remove(dataSourceId);
				
				logger.debug("数据源[{}]，加载运行结束", dataSourceId);
			} catch (InterruptedException e) {
				logger.debug("数据源加载后续线程运行中断，准备退出");
				break;
			} catch (Exception e) {
				logger.error("数据源加载后续运行异常", e);
			}
		}
		
		logger.debug("数据源加载后续线程运行结束");
	}

}
