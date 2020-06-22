package com.zdawn.commons.pv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcMessageQueue implements MessageQueue {
	private static Logger logger = LoggerFactory.getLogger(JdbcMessageQueue.class);
	/**
	 * 队列最大深度
	 */
	private int maxSize = 200;
	/**
	 * message 队列
	 */
	private LinkedList<MessageWrapper> queueMsg = new LinkedList<MessageWrapper>();
	/**
	 * 存储消息表名
	 */
	private String msgStoreTableName;
	/**
	 * 存储消息数据库源
	 */
	private DataSource dataSource; 
	/**
	 * 消息处理次数
	 */
	private int msgHandleTimes = 3;
	/**
	 * message full class name 
	 */
	private String msgClazzName;
	
	private HandlingMediator mediator;
	/**
	 * 获取消息
	 */
	public synchronized MessageWrapper pollMessage(){
		try {
			while(queueMsg.size()==0){
				wait();
			}
		} catch (InterruptedException e) {
			logger.error("pollMessage",e);
		}
		return queueMsg.removeFirst();
	}
	/**
	 * 添加消息
	 */
	public synchronized boolean putMessage(Message msg){
		if(mediator.getBreakerStatus()==1){
			logger.warn("breaker status is open,can not put message");
			return false;
		}
		if(queueMsg.size()>=maxSize){
			logger.warn("message queue size reach max size,can not put message");
			return false;
		}
		//保存消息到数据库
		try {
			String id = saveMsg(msg);
			MessageWrapper wrapper = new MessageWrapper(msg);
			wrapper.putAttribute("msgId", id);
			queueMsg.add(wrapper);
		} catch (Exception e) {
			logger.error("putMessage",e);
			return false;
		}
		notifyAll();
		return true;
	}
	private String saveMsg(Message msg)throws Exception{
		String sql = "insert into "+msgStoreTableName+"(id,content,create_time,exec_count,msg_state) values(?,?,?,?,?)";
		Connection conn = null;
		PreparedStatement ps = null;
		String id = UUID.randomUUID().toString();
		try {
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setString(1,id);
			byte[] data = msg.exportMessage();
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
		    ps.setBinaryStream(2, bis, data.length);
			ps.setTimestamp(3,new Timestamp(System.currentTimeMillis()));
			ps.setInt(4,0);
			ps.setInt(5,1);
			ps.executeUpdate();
		} catch (Exception e) {
			logger.error("saveMsg", e);
			throw e;
		} finally {
			closeStatement(ps);
			closeConnection(conn);
		}
		return id;
	}
	private void removeMsg(String msgId){
		String sql = "delete from "+msgStoreTableName+" where id=?";
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(sql);
			ps.setString(1,msgId);
			ps.executeUpdate();
		} catch (Exception e) {
			logger.error("msgId="+msgId+" delete error!");
			logger.error("removeMsg", e);
		} finally {
			closeStatement(ps);
			closeConnection(conn);
		}
	}
	private void updateMsgExecFailure(String msgId){
		String selectSql = "select exec_count,msg_state from "+msgStoreTableName+" where id=?";
		String updateSql = "update "+msgStoreTableName+" set msg_state=?,exec_count=?,exec_finishtime=? where id=?";
		Connection conn = null;
		PreparedStatement ps = null;
		PreparedStatement psUpdate = null;
		ResultSet rs = null;
		try {
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(selectSql);
			ps.setString(1,msgId);
			rs=ps.executeQuery();
			if(!rs.next()) throw new Exception("msgId="+msgId+ " is not exist");
			int execCount = rs.getInt(1);
			int msgState = rs.getInt(2);
			if(msgState!=1){
				logger.warn("message state is not correct. msgState="+msgState);
			}
			execCount = execCount +1;
			if(execCount>=msgHandleTimes){
				msgState = 3;
			}else{
				msgState = 2;
			}
			psUpdate = conn.prepareStatement(updateSql);
			psUpdate.setInt(1,msgState);
			psUpdate.setInt(2,execCount);
			psUpdate.setTimestamp(3,new Timestamp(System.currentTimeMillis()));
			psUpdate.setString(4,msgId);
			psUpdate.executeUpdate();
		} catch (Exception e) {
			logger.error("updateMsgExecFailure", e);
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeStatement(psUpdate);
			closeConnection(conn);
		}
	}
	private void loadMsgToQueue(String msgId,Connection conn){
		String selectSql = "select exec_count,msg_state,content from "+msgStoreTableName+" where id=?";
		String updateSql = null;
		PreparedStatement ps = null;
		PreparedStatement psUpdate = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(selectSql);
			ps.setString(1,msgId);
			rs=ps.executeQuery();
			if(!rs.next()) throw new Exception("msgId="+msgId+ " is not exist");
			int execCount = rs.getInt(1);
			int msgState = rs.getInt(2);
			if(msgState!=0 && msgState!=2) throw new Exception("msgId="+msgId+ " message state is not correct msgState="+msgState);
			byte[] content = readContent(rs);
			if(content==null) throw new Exception("msgId="+msgId+ " message is empty");
			//执行次数大于设置值
			if(execCount>=msgHandleTimes){
				//状态转为错误消息
				updateSql = "update "+msgStoreTableName+" set msg_state=? where id=?";
				psUpdate = conn.prepareStatement(updateSql);
				psUpdate.setInt(1,3);
				psUpdate.setString(2,msgId);
				psUpdate.executeUpdate();
			}else{//添加队列
				if(putMessage(msgId, content)){
					updateSql = "update "+msgStoreTableName+" set msg_state=? where id=?";
					psUpdate = conn.prepareStatement(updateSql);
					psUpdate.setInt(1,1);
					psUpdate.setString(2,msgId);
					psUpdate.executeUpdate();
				}
			}
		} catch (Exception e) {
			logger.error("loadMsgToQueue", e);
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeStatement(psUpdate);
		}
	}
	//read message content
	private byte[] readContent(ResultSet rs) throws Exception {
		byte[] content = null;
		try(InputStream is = rs.getBinaryStream(3)){
			ByteArrayOutputStream os  = new ByteArrayOutputStream();
			byte[] buf = new byte[512];
			int bytesRead = -1;
			while((bytesRead = is.read(buf))!=-1) {
				os.write(buf, 0, bytesRead);
			}
			content = os.toByteArray();
		} catch (Exception e) {
			throw e;
		}
		return content;
	}
	
	private void updateMsgNonexecution() throws Exception{
		String sql="update "+msgStoreTableName+" set msg_state=0 where msg_state=1";
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(sql);
			ps.executeUpdate();
		} catch (Exception e) {
			logger.error("updateMsgNonexecution", e);
			throw e;
		} finally {
			closeStatement(ps);
			closeConnection(conn);
		}
	}
	private List<String> loadPendingMsgId(Connection conn){
		List<String> list = new ArrayList<String>();
		String sql = "select id from "+msgStoreTableName+" where msg_state in(0,2) order by create_time";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				list.add(rs.getString(1));
				if(list.size()>maxSize) break;
			}
		} catch (Exception e) {
			logger.error("loadPendingMsgId", e);
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
		}
		return list;
	}
	private synchronized boolean putMessage(String id,byte[] content){
		if(mediator.getBreakerStatus()==1){
			logger.warn("breaker status is open,can not put message");
			return false;
		}
		if(queueMsg.size()>=maxSize){
			logger.warn("message queue size reach max size,can not put message");
			return false;
		}
		try {
			//load data
			Class<?> clazz = getClass().getClassLoader().loadClass(msgClazzName);
			Message msg = (Message)clazz.newInstance();
			msg.importMessage(content);
			MessageWrapper wrapper = new MessageWrapper(msg);
			wrapper.putAttribute("msgId",id);
			queueMsg.add(wrapper);
		} catch (Exception e) {
			logger.error("putMessage",e);
			return false;
		}
		notifyAll();
		return true;
	}
	/**
	 * 能否添加任务
	 */
	public boolean canAddMessage(){
		return queueMsg.size() < maxSize;
	}
	/**
	 * 获取当前任务队列深度
	 */
	public int getCurrentQueueSize(){
		return queueMsg.size();
	}

	public int getMaxSize() {
		return maxSize;
	}
	
	public void init() {
		if(mediator==null) throw new RuntimeException("HandlingMediator is not setting");
		//消息状态为执行中消息更新为未执行
		try {
			updateMsgNonexecution();
		} catch (Exception e) {}
	}
	
	public void loadPendingMsgToQueue(){		
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
			//load pending message id
			List<String> list = loadPendingMsgId(conn);
			for (String id : list) {
				loadMsgToQueue(id, conn);
			}
		} catch (Exception e) {
			logger.error("loadPendingMsgToQueue", e);
		} finally {
			closeConnection(conn);
		}
	}
	
	public void onHandleMsgResult(boolean success, MessageWrapper msgWrapper) {
		Object obj = msgWrapper.getAttribute("msgId");
		if(obj==null || "".equals(obj)){
			logger.warn("the onHandleMsgResult method msgId is empty");
			return;
		}
		String msgId = obj.toString();
		if(success){
			removeMsg(msgId);
		}else{
			updateMsgExecFailure(msgId);
		}
	}
	private void closeStatement(Statement stmt){
		try {
			if(stmt!=null) stmt.close();
		} catch (SQLException e) {
			logger.error("closeStatement",e);
		}
	}
	private void closeResultSet(ResultSet rs){
		try {
			if(rs!=null) rs.close();
		} catch (SQLException e) {
			logger.error("closeSResultSet",e);
		}
	}
	private void closeConnection(Connection connection){
		try {
			if(connection !=null) connection.close();
		} catch (Exception e) {
			logger.error("closeConnection",e);
		}
	}
	
	public void configureMediator(HandlingMediator mediator) {
		this.mediator = mediator;
	}
	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}
	public void setMsgStoreTableName(String msgStoreTableName) {
		this.msgStoreTableName = msgStoreTableName;
	}
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	public void setMsgHandleTimes(int msgHandleTimes) {
		this.msgHandleTimes = msgHandleTimes;
	}
	public void setMsgClazzName(String msgClazzName) {
		this.msgClazzName = msgClazzName;
	}
}
