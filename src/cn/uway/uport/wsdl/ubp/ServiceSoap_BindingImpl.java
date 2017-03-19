/**
 * ServiceSoap_BindingImpl.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package cn.uway.uport.wsdl.ubp;

import java.rmi.RemoteException;

import javax.xml.rpc.holders.BooleanHolder;
import javax.xml.rpc.holders.IntHolder;
import javax.xml.rpc.holders.StringHolder;

import cn.uway.uport.ubp.InterfaceOpertionImpl;

public class ServiceSoap_BindingImpl implements cn.uway.uport.wsdl.ubp.ServiceSoap_PortType {

	@Override
	public int addMonitTask(String xmlStrTask, String xmlStrSecure, StringHolder msg) throws RemoteException {
		return InterfaceOpertionImpl.getInstance().addMonitorTask(xmlStrTask, xmlStrSecure, msg);
	}

	@Override
	public void showMonitTaskStatusById(String monitTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)
			throws RemoteException {
		InterfaceOpertionImpl.getInstance().showMonitorTaskStatusById(monitTaskId, xmlStrSecure, xmlStrResponse, status);
	}

	@Override
	public void showOwnerMonitTasks(String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status) throws RemoteException {
		InterfaceOpertionImpl.getInstance().showOwnerMonitorTasks(xmlStrSecure, xmlStrResponse, status);
	}

	@Override
	public int deleteMonitTask(String monitTaskId, String xmlStrSecure) throws RemoteException {
		return InterfaceOpertionImpl.getInstance().deleteMonitorTask(monitTaskId, xmlStrSecure);
	}

	@Override
	public int deleteMonitTaskWithoutAlarmClear(String monitTaskId, String xmlStrSecure) throws RemoteException {
		return InterfaceOpertionImpl.getInstance().deleteMonitorTaskWithoutAlarmClear(monitTaskId, xmlStrSecure);
	}

	@Override
	public int modifyMonitTask(String monitTaskId, String xmlStrNewTask, String xmlStrSecure, StringHolder msg) throws RemoteException {
		return InterfaceOpertionImpl.getInstance().modifyMonitorTask(monitTaskId, xmlStrNewTask, xmlStrSecure, msg);
	}

	@Override
	public int setMonitorTaskStatus(String monitorTaskId, String xmlStrSecure, Integer oper) throws RemoteException {
		return InterfaceOpertionImpl.getInstance().setMonitorTaskStatus(monitorTaskId, xmlStrSecure, oper);
	}

	@Override
	public int existsMonitTask(String monitTaskId, String xmlStrSecure, BooleanHolder result, StringHolder msg) throws RemoteException {
		return InterfaceOpertionImpl.getInstance().existsMonitorTask(monitTaskId, xmlStrSecure, result, msg);
	}
	
	@Override
	public void showMonitorTask(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status) throws RemoteException {
		InterfaceOpertionImpl.getInstance().showMonitorTask(monitorTaskId, xmlStrSecure, xmlStrResponse, status);
	}

}
