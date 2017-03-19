package cn.uway.uport;

import java.io.Console;
import java.io.File;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uway.uport.context.ApplicationContext;
import cn.uway.uport.context.Configuration;
import cn.uway.uport.context.ProgramDefineValidator;
import cn.uway.uport.context.SecurityValidator;

/**
 * <pre>
 * UBP接口部分启动类
 * 负责启动UPORT以便接受外部下发的请求
 * 目前下发请求主要是通过web service方式
 * 
 * @author Chris @ 2013-10-31
 * </pre>
 */
public class Runner {

	private static final String WEB_APP = "WebRoot";

	private static final String CONTEXT_PATH = "/";

	private static final String VERSION_CODE = "1.3.3.0-160902";

	/**
	 * 日志
	 */
	private static final Logger logger = LoggerFactory.getLogger(Runner.class);

	/**
	 * <pre>
	 * UPORT启动方法
	 * 1、初始化配置文件
	 * 2、初始化程序功能模块
	 * 3、启动控制台
	 * 
	 * @param args
	 * @throws Exception 如果抛出异常,UPORT将无法启动
	 * </pre>
	 */
	public static void main(String[] args) throws Exception {
		// 应用程序根目录
		String dir = null;
		if (args != null && args.length == 1)
			dir = args[0];

		// 初始化程序上下文
		ApplicationContext.initialize(dir);
		// 启动web容器
		startWeb();
		// 命令行
		processCmd();

		logger.info("UPort启动完成");
	}

	/**
	 * <pre>
	 * 启动web容器
	 * 
	 * @param dir 应用程序根目录
	 * @throws Exception
	 * </pre>
	 */
	private static final void startWeb() throws Exception {
		// 启动Web容器
		int port = Configuration.getInteger(Configuration.PORT);
		Server server = new Server(port);
		File file = null;
		if ("".equals(Configuration.ROOT_DIRECTORY)) {
			file = new File(WEB_APP);
		} else {
			file = new File(Configuration.ROOT_DIRECTORY, WEB_APP);
		}

		WebAppContext ctx = new WebAppContext(file.getPath(), CONTEXT_PATH);
		server.addHandler(ctx);
		server.start();
		logger.info("WEB端口已启动，端口：{}，上下文路径：{}", new Object[]{port, CONTEXT_PATH});
	}

	/**
	 * <pre>
	 * 命令行处理 用于程序运行过程中,对程序进行热操作,如刷新安全校验信息等
	 * 
	 * @throws Exception
	 * </pre>
	 */
	private static void processCmd() {
		Console console = System.console();
		if (console == null) {
			logger.warn("无法访问与当前JVM关联的基于字符的控制台设备");
			return;
		}

		StringBuilder sb = new StringBuilder();
		sb.append("刷新安全校验信息:'refresh security data'或'r sd'");
		sb.append("刷新程序定义信息:'refresh program define'或'r pd'");
		sb.append("需要指令帮助:'help'或'?'");
		String cmdStr = sb.toString();
		String cmd;

		while (true) {
			cmd = console.readLine();
			switch (cmd) {
				case "refresh security data" :
				case "r sd" :
					try {
						SecurityValidator.loadSecurityInfo();
						console.printf("刷新安全校验信息成功\n");
					} catch (Exception e) {
						console.printf("刷新安全校验信息失败%s", e.getMessage());
						logger.error("刷新安全校验信息失败", e);
					}

					break;
				case "refresh program define" :
				case "r pd" :
					try {
						ProgramDefineValidator.loadAllProgramDefine();
						console.printf("刷新程序定义信息成功\n");
					} catch (Exception e) {
						console.printf("刷新程序定义信息失败%s", e.getMessage());
						logger.error("刷新程序定义信息失败", e);
					}

					break;
				case "help" :
					break;
				case "version" :
					console.printf("当前版本号:" + VERSION_CODE);
					break;
				case "?" :
					console.printf(cmdStr);
					break;
				default :
					console.printf("不能识别的指令:%s\n可用指令:%s\n", cmd, cmdStr);
			}
		}
	}
}
