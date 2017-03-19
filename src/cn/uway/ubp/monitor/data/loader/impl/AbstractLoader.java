package cn.uway.ubp.monitor.data.loader.impl;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.framework.util.database.DatabaseUtil;
import cn.uway.ubp.monitor.context.Configuration;
import cn.uway.ubp.monitor.context.DbPoolManager;
import cn.uway.ubp.monitor.dao.DataSourceDAO;
import cn.uway.ubp.monitor.dao.DataSourceStatusDAO;
import cn.uway.ubp.monitor.data.BlockData;
import cn.uway.ubp.monitor.data.BlockDataProviderImpl;
import cn.uway.ubp.monitor.data.future.LoadFuture;
import cn.uway.ubp.monitor.data.loader.Loader;
import cn.uway.ubp.monitor.data.loader.impl.database.DbLoader;
import cn.uway.ubp.monitor.data.loader.impl.file.FileLoader;
import cn.uway.util.DateGranularityUtil;
import cn.uway.util.entity.DataSource;

/**
 * <pre>
 * 抽象的加载器
 * 使用模版方法模式 规范数据加载的流程
 * 具体的子类只需要实现具体的load即可
 * 
 * @author Chris 2014-3-11
 * </pre>
 */
public abstract class AbstractLoader implements Loader {

	protected DataSource dataSource;

	private DataSourceStatusDAO dssDAO = DataSourceStatusDAO.getInstance();

	// 用于保存历次加载数据源数据的记录数
	private static Map<Long, Integer> dataSizeMap = new ConcurrentHashMap<>();

	// 用于保存数据源首次加载时间，处理非日志驱动类数据源加载超时的机制
	private static Map<Long, Long> loadStartTimeMap = new ConcurrentHashMap<>();

	// 最长加载运行时间
	private static long MAX_LOAD_TIME = Configuration
			.getInteger(Configuration.DATASOURCE_MAX_LOAD_TIME) * 60 * 1000L;

	// 非日志驱动类数据源，单一数据时间点加载超时时间
	private static long LOAD_TIMEOUT = Configuration
			.getInteger(Configuration.DATASOURCE_LOAD_TIMEOUT) * 60 * 1000L;

	private static final Logger logger = LoggerFactory
			.getLogger(AbstractLoader.class);

	public static AbstractLoader buildLoader(Connection taskConn, DataSource dataSource)
			throws Exception {
		AbstractLoader loader = null;

		switch (dataSource.getType()) {
			case 0 :
				loader = DbLoader.buildLoader(taskConn, dataSource);
				break;
			case 1 :
				loader = FileLoader.buildLoader(taskConn, dataSource);
				break;
			default :
				throw new Exception("未知的类型" + dataSource.getType());
		}

		return loader;
	}

	public AbstractLoader(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public DataSource getDatasource() {
		return dataSource;
	}

	// TODO 待重构，简化逻辑
	@Override
	public LoadFuture call() {
		Thread.currentThread().setName("数据源加载"+dataSource.getId());

		long startTime = System.currentTimeMillis();

		long dataSourceId = dataSource.getId();
		LoadFuture future = new LoadFuture();
		future.setDataSourceId(dataSourceId);

		BlockData blockData;
		int currSize;
		int status;
		String cause;
		Timestamp dataTime;
		logger.debug("数据源[{}]，开始加载", dataSourceId);

		do {
			blockData = null;
			currSize = -1;
			status = 0;
			cause = null;

			// STEP1，查找可用的数据时间点
			try {
				dataTime = getAvailableDataTime(dataSource.getDataTime());
				if (dataTime == null) {
					logger.debug("数据源[{}]，没有可用的数据时间点：{}", new Object[]{
							dataSourceId, dataSource.getDataTime()});
					break;
				}
			} catch (Exception e) {
				logger.error("数据源[{}]，查找可用的数据时间点失败", dataSource, e);
				break;
			}

			// STEP2，加载
			Connection taskConn = null;
			try {
				taskConn = DbPoolManager.getConnectionForTask();
				dssDAO.addDataSourceStatus(taskConn, dataSourceId, dataTime);

				try {
					logger.debug("数据源[{}]，数据时间[{}]，开始加载...", dataSourceId,
							dataTime);
					dssDAO.updateDataSourceStatusForStartLoad(taskConn, dataSourceId,
							dataTime);
					// 由于load()可能耗费很长时间，所以暂时释放，等load返回再重新获取
					DatabaseUtil.close(taskConn);
					
					blockData = load();

					if (blockData == null) {
						cause = "加载数据为空";
						break;
					}

					taskConn = DbPoolManager.getConnectionForTask();
					currSize = blockData.getGroupingArrayDatas().size();
					if (currSize == 0) {
						if (dataSource.isLogDrive()) {
							cause = "加载数据为0";
							status = 1;
						} else {
							// @20140408，添加单一数据时间点最长加载超时时间判断
							if (loadStartTimeMap.containsKey(dataSourceId)) {
								long time = System.currentTimeMillis()
										- loadStartTimeMap.get(dataSourceId);
								if (LOAD_TIMEOUT > 0 && time >= LOAD_TIMEOUT) {
									// 加载超时，清除之前的加载记录，更新到下一个数据源时间点
									loadStartTimeMap.remove(dataSourceId);
									updateDataSourceTime(taskConn, dataSourceId,
											dataTime, dataSource
													.getGranularity()
													.toString());
									logger.debug(
											"数据源[{}]是非日志驱动类数据源，加载数据时间[{}]超过最大超时时间[{}]，已跳到下一个数据时间点",
											new Object[]{
													dataSourceId,
													dataTime,
													Configuration
															.getInteger(Configuration.DATASOURCE_LOAD_TIMEOUT)});

									// 这里break要留着，不然会更新两次时间
									break;
								} else {
									cause = "加载数据为0";
									status = 1;
									break;
								}
							} else {
								loadStartTimeMap.put(dataSourceId,
										System.currentTimeMillis());
								cause = "加载数据为0";
								status = 1;
								break;
							}
						}

						if (!isOutOfLoadCheckTimeRange()) {
							break;
						}
					} else {
						// @20140408，加载成功，清除之前的加载记录，更新到下一个数据源时间点
						if (!dataSource.isLogDrive()) {
							loadStartTimeMap.remove(dataSourceId);
						}
					}

					logger.debug("数据源[{}]，数据时间[{}]，完成加载，数据量[{}]", new Object[]{
							dataSourceId, dataTime, currSize});
					dssDAO.updateDataSourceStatusForEndLoad(taskConn, dataSourceId,
							dataTime, currSize, cause);
				} catch (Exception e) {
					logger.error("加载数据失败", e);
					cause = e.getMessage();
					break;
				}

				// STEP3，检查数据完整性，阀值配置<0，表示不启用
				float percent = Configuration
						.getFloat(Configuration.DATASOURCE_FAULT_TOLERANCE_PERCENT);
				if (percent >= 0 && checkDataSize(taskConn, currSize, percent,dataTime)) {
					if (!isOutOfLoadCheckTimeRange()) {
						logger.warn(
								"数据源[{}]，数据时间[{}]，本次加载的数据量与上次比较超出偏差阀值{}",
								new Object[]{dataSource.getId(),
										dataSource.getDataTime(), percent});
						cause = "数据源" + dataSource.getId() + "数据时间"
								+ dataSource.getDataTime()
								+ "本次加载的数据量与上次比较超出偏差阀值" + percent;

						break;
					}
				}

				// STEP4，序列化
				try {
					if (currSize > 0) {
						dssDAO.updateDataSourceStatusForStartStore(
								taskConn, dataSourceId, dataTime);
						int flag = BlockDataProviderImpl.getInstance().save(
								dataSource, blockData);

						if (flag < 0) {
							logger.error("数据源[{}]，数据时间[{}]，数据序列化失败",
									new Object[]{dataSourceId, dataTime});

							cause = "数据序列化失败";
							dssDAO.updateDataSourceStatusForEndStore(
									taskConn, dataSource.getId(),
									dataSource.getDataTime(), cause);
							break;
						} else {
							dssDAO.updateDataSourceStatusForEndStore(
									taskConn, dataSource.getId(),
									dataSource.getDataTime(), null);
						}
					}
				} catch (Exception e) {
					logger.error("序列化数据失败", e);

					cause = e.getMessage();
					dssDAO.updateDataSourceStatusForEndStore(
							taskConn, dataSource.getId(), dataSource.getDataTime(), cause);
					break;
				}

				// STEP5，更新数据源时间为下一个时间粒度
				try {
					Timestamp nextDataTime = updateDataSourceTime(taskConn, dataSourceId,
							dataTime, dataSource.getGranularity().toString());
					if (nextDataTime == null) {
						logger.warn("数据源[{}]，数据时间[{}]，更新数据时间失败", new Object[]{
								dataSourceId, dataTime});
						break;
					}

					logger.debug("数据源[{}]，数据时间[{}]，更新数据时间至：{}", new Object[]{
							dataSourceId, dataTime, nextDataTime});
				} catch (Exception e) {
					logger.error("数据源[{}]，数据时间[{}]，更新数据时间失败", new Object[]{
							dataSourceId, dataTime, e});
					break;
				}

				// STEP6，完成加载
				finishLoad();

				status = 1;
				DataSourceStatusDAO.getInstance()
				.updateDataSourceStatusForEndRun(taskConn, dataSourceId,
						dataTime, status);
			} catch (Exception e) {
				logger.error("数据源[{}]，数据时间[{}]，加载失败", new Object[]{
						dataSourceId, dataTime, e});
				break;
			} finally {
				DatabaseUtil.close(taskConn);
			}
		} while ((System.currentTimeMillis() - startTime) < MAX_LOAD_TIME); // 在超时范围内，继续加载

		logger.debug("数据源[{}]，加载完成，耗时{}ms", dataSourceId,
				(System.currentTimeMillis() - startTime));
		return future;
	}

	/**
	 * <pre>
	 * 检查数据偏差是否赶出阀值
	 * 
	 * @param currSize 本次加载数量
	 * @return 数据偏差值超过阀值则返回true，否则返回false
	 * </pre>
	 */
	private boolean checkDataSize(Connection conn, int currSize, float percent, Timestamp dataTime) {
		int lastSize = 0;
		if (dataSizeMap.containsKey(dataSource.getId())) {
			lastSize = dataSizeMap.get(dataSource.getId());
		}else{
			lastSize = dssDAO.getMaxLoadCount(conn, dataSource.getId(), dataTime);
		}
		// 波动比例的最低数
		float _v = percent / 100 * lastSize;
		
		/**
		 * <pre>
		 *  temporary change:sg
		 *  date: 2014-04-29
		 * 	explain: 记录数如果比原来多，则直接通过，如果本次加载的数据条数小于比例就不通过.
		 * </pre>
		 */
		if ((currSize < lastSize) && ((lastSize - currSize) > _v)) {
			return true;
		}

		dataSizeMap.put(dataSource.getId(), currSize);
		return false;
	}

	/**
	 * <pre>
	 * 数据源时间下一个时间粒度
	 * 
	 * @param dataSourceId
	 * @param currDateTime
	 * @param gran
	 * @return 修改成功，返回下一个时间粒度，否则返回null
	 * @throws Exception
	 * </pre>
	 */
	private Timestamp updateDataSourceTime(Connection conn, long dataSourceId,
			Timestamp currDateTime, String gran) throws Exception {
		Timestamp nextDataTime = new Timestamp(DateGranularityUtil
				.forwardTimeTravel(currDateTime, gran, 1).getTime());

		boolean b = DataSourceDAO.getInstance().updateSourceDatetime(
				conn, dataSourceId, nextDataTime, currDateTime);
		if (b) {
			dataSource.setDataTime(nextDataTime);
			return nextDataTime;
		} else {
			return null;
		}
	}

	/**
	 * 是否超出了加载检查时间限制。
	 * 
	 * <pre>
	 * 	如果超过了一个固定检查时间，就不需要作数据条数正确，和数据是否存在限制
	 * 	//TODO:这个方案也有缺陷，如果数据库汇总被停掉,UBP的数据源时间将会被超过实际时间加载，
	 * 			另外对于有长时间的空缺数据，也会被按数据源粒度，逐时加载，影响效率。
	 * </pre>
	 * 
	 * @return
	 */
	private boolean isOutOfLoadCheckTimeRange() {
		int checkHours = Configuration
				.getInteger(Configuration.DATASOURCE_FAULT_TOLERANCE_PERCENT_CHECK_HOURS);
		if (checkHours < 1)
			checkHours = 1;

		// 最后一次检验时间 = 当前数据源加载时间向未来推一个单位;
		Timestamp lastCheckTime = new Timestamp(DateGranularityUtil
				.forwardTimeTravel(dataSource.getDataTime(),
						dataSource.getGranularity().toString(), 1).getTime());
		// 最后一次检验时间 + N个小时(延时).
		lastCheckTime = new Timestamp(lastCheckTime.getTime() + checkHours
				* (60 * 60 * 1000L));
		// TODO: currSysTime理想是取数据库的sysdate时间，暂时取pc时间
		Timestamp currSysTime = new Timestamp(System.currentTimeMillis());

		// 如果最后检查时间在系统时间之前，则代表该数据源的当前时间，已经无重试加载必要.
		return lastCheckTime.before(currSysTime);
	}

}
