package cn.uway.ubp.monitor.data.task;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.framework.util.thread.ExecutorThreadPool;
import cn.uway.ubp.monitor.context.DbPoolManager;
import cn.uway.ubp.monitor.dao.DataSourceDAO;
import cn.uway.ubp.monitor.data.future.LoadFuture;
import cn.uway.ubp.monitor.data.loader.impl.AbstractLoader;
import cn.uway.util.entity.DataSource;

/**
 * 数据源加载调度
 * @author chris
 * @ 2014年3月10日
 */
public class LoadTask implements Runnable {
	
	/**
	 * 数据源加载线程池
	 */
	private ExecutorThreadPool<LoadFuture> loaderThreadPool;

	/**
	 * 加载数据源,保证数据源被加载唯一性，同一时间不会被重复加载
	 */
	private ConcurrentMap<Long, DataSource> loadingMap;
	
	private static final Logger logger = LoggerFactory.getLogger(LoadTask.class);

	public LoadTask(ExecutorThreadPool<LoadFuture> loaderThreadPool, ConcurrentMap<Long, DataSource> loadingMap) {
		this.loaderThreadPool = loaderThreadPool;
		this.loadingMap = loadingMap;
	}

	@Override
	public void run() {
		Thread.currentThread().setName("数据源加载调度");
		Connection taskConn = null;
		try {
			taskConn = DbPoolManager.getConnectionForTask();
			List<DataSource> dataSourceList = DataSourceDAO.getInstance().getDataSources(taskConn);
			if (dataSourceList == null || dataSourceList.isEmpty()) {
				logger.debug("未找到对应的数据源配置");
				return;
			}

			logger.debug("共找到[{}]个需要加载的数据源", dataSourceList.size());
			
			long dataSourceId;
			for (DataSource dataSource : dataSourceList) {
				dataSourceId = dataSource.getId();
				
				// 保证数据源不被重复加载
				if (loadingMap.containsKey(dataSourceId)) {
					logger.debug("数据源[{}]，正在加载，本次调度忽略", dataSourceId);
					continue;
				}
				
				try {
					AbstractLoader loader = AbstractLoader.buildLoader(taskConn, dataSource);
					loaderThreadPool.submit(loader);
					loadingMap.put(dataSourceId, dataSource);
					
					logger.debug("数据源[{}]，已经提交至加载队列", dataSourceId);
				} catch (Exception e) {
					logger.error("数据源[{}]，提交加载队列失败", dataSourceId, e);
				}
			}
		} catch (Exception e) {
			logger.error("数据源加载调度运行异常", e);
		}finally {
			DatabaseUtil.close(taskConn);
		}
	}

}
