/**
 * ServiceSoap_PortType.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package cn.uway.uport.wsdl.ubp;

import java.rmi.Remote;
import java.rmi.RemoteException;

import javax.xml.rpc.holders.BooleanHolder;
import javax.xml.rpc.holders.IntHolder;
import javax.xml.rpc.holders.StringHolder;

public interface ServiceSoap_PortType extends Remote {

	public int addMonitTask(String xmlStrTask, String xmlStrSecure, StringHolder msg) throws RemoteException;

	public void showMonitTaskStatusById(String monitTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status)
			throws RemoteException;

	public void showOwnerMonitTasks(String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status) throws RemoteException;

	public int deleteMonitTask(String monitTaskId, String xmlStrSecure) throws RemoteException;

	public int deleteMonitTaskWithoutAlarmClear(String monitTaskId, String xmlStrSecure) throws RemoteException;

	public int modifyMonitTask(String monitTaskId, String xmlStrNewTask, String xmlStrSecure, StringHolder msg) throws RemoteException;

	public int setMonitorTaskStatus(String monitorTaskId, String xmlStrSecure, Integer oper) throws RemoteException;

	public int existsMonitTask(String monitTaskId, String xmlStrSecure, BooleanHolder result, StringHolder msg) throws RemoteException;
	
	public void showMonitorTask(String monitorTaskId, String xmlStrSecure, StringHolder xmlStrResponse, IntHolder status) throws RemoteException;

}
