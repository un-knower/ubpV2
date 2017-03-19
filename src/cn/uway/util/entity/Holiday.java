package cn.uway.util.entity;

/**
 * 节假日实体类
 * 
 * @author Chris @ 2013-11-1
 */
public class Holiday {

	/**
	 * 任务节假日策略
	 */
	private int policy;

	/**
	 * 数据节假日策略
	 */
	private int strategy;

	public Holiday(int policy, int strategy) {
		super();
		this.policy = policy;
		this.strategy = strategy;
	}

	public int getPolicy() {
		return policy;
	}

	public void setPolicy(int policy) {
		this.policy = policy;
	}

	public int getStrategy() {
		return strategy;
	}

	public void setStrategy(int strategy) {
		this.strategy = strategy;
	}

}
