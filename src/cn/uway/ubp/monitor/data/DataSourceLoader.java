package cn.uway.ubp.monitor.data;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import cn.uway.framework.util.thread.ExecutorThreadPool;
import cn.uway.ubp.monitor.context.Configuration;
import cn.uway.ubp.monitor.data.future.LoadFuture;
import cn.uway.ubp.monitor.data.task.LoadTask;
import cn.uway.util.entity.DataSource;

/**
 * 数据源加载器
 * 调度数据源加载、添加至线程池加载
 * @author chris
 * @ 2014年3月10日
 */
public class DataSourceLoader {
	/**
	 * 数据源加载调度
	 */
	private ScheduledExecutorService dispather;

	/**
	 * 数据源加载线程池
	 */
	private ExecutorThreadPool<LoadFuture> loaderThreadPool;

	/**
	 * 数据源加载完成后续处理
	 */
	private CompletionProcessor completionProcessor;

	/**
	 * 正在加载的数据源，保证数据源被加载唯一性，同一时间不会被重复加载
	 */
	private ConcurrentMap<Long, DataSource> loadingMap;
	
	public DataSourceLoader() {
		dispather = Executors.newSingleThreadScheduledExecutor();
		
		int threadPoolSize = Configuration.getInteger(Configuration.DATASOURCE_LOAD_MAX_THREADS);
		loaderThreadPool = new ExecutorThreadPool<>(threadPoolSize);
		
		loadingMap = new ConcurrentHashMap<Long, DataSource>();
		
		completionProcessor = new CompletionProcessor(loaderThreadPool, loadingMap);
	}
	
	/**
	 * 获取当前正在加载的数据源
	 * @return 正在加载的数据源列表
	 */
	public Collection<DataSource> getLoadingDataSource() {
		return loadingMap.values();
	}
	
	/**
	 * 查询指定的数据源是否正在加载
	 * @param id 数据源Id
	 * @return
	 */
	public boolean isLoading(long id) {
		return loadingMap.containsKey(id);
	}
	
	/**
	 * 启用加载器
	 */
	public void startup() {
		completionProcessor.startup();
		
		int period = Configuration.getInteger(Configuration.DATASOURCE_LOAD_PERIOD);
		dispather.scheduleAtFixedRate(new LoadTask(loaderThreadPool, loadingMap), 0, period, TimeUnit.MINUTES);
	}
	
	/**
	 * 关闭加载器
	 */
	public void shutdown() {
		dispather.shutdown();
		loaderThreadPool.shutdown();
		completionProcessor.shutdown();
		loadingMap.clear();
	}
	
	/**
	 * 马上中止正在运行的加载器线程
	 */
	public void shutdownNow() {
		dispather.shutdownNow();
		loaderThreadPool.shutdownNow();
		completionProcessor.shutdown();
		loadingMap.clear();
	}

}
