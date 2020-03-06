/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.server.quorum;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * This class is the superclass of two of the three main actors in a ZK
 * ensemble: Followers and Observers. Both Followers and Observers share 
 * a good deal of code which is moved into Peer to avoid duplication. 
 */
public class Learner {       
    static class PacketInFlight {
        TxnHeader hdr; //事务头
        Record rec; //记录
    }
    QuorumPeer self; //当前集群节点
    LearnerZooKeeperServer zk;//当前learner状态的zk服务器
    
    protected BufferedOutputStream bufferedOutput;
    
    protected Socket sock;
    
    /**
     * Socket getter
     * @return 
     */
    public Socket getSocket() {
        return sock;
    }
    
    protected InputArchive leaderIs;//输入
    protected OutputArchive leaderOs;  //输出
    /** the protocol version of the leader */
    protected int leaderProtocolVersion = 0x01;//协议版本号
    
    protected static final Logger LOG = LoggerFactory.getLogger(Learner.class);

    static final private boolean nodelay = System.getProperty("follower.nodelay", "true").equals("true");
    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }   
    
    final ConcurrentHashMap<Long, ServerCnxn> pendingRevalidations =
        new ConcurrentHashMap<Long, ServerCnxn>();//client连接到learner时，learner要向leader提出REVALIDATE请求，在收到回复之前，记录在一个map中，表示尚未处理完的验证
    
    public int getPendingRevalidationsCount() {
        return pendingRevalidations.size();
    }
    
    /**
     * validate a session for a client
     *
     * @param clientId
     *                the client to be revalidated
     * @param timeout
     *                the timeout for which the session is valid
     * @return
     * @throws IOException
     */
    void validateSession(ServerCnxn cnxn, long clientId, int timeout) //校验session
            throws IOException {
        LOG.info("Revalidating client: 0x" + Long.toHexString(clientId));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(clientId);//写入客户端id
        dos.writeInt(timeout);
        dos.close();
        QuorumPacket qp = new QuorumPacket(Leader.REVALIDATE, -1, baos
                .toByteArray(), null); //重连命令包
        pendingRevalidations.put(clientId, cnxn);//需要验证，还未返回结果，记录在map中
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                                     ZooTrace.SESSION_TRACE_MASK,
                                     "To validate session 0x"
                                     + Long.toHexString(clientId));
        }
        writePacket(qp, true);//发送
    }     
    
    /**
     * write a packet to the leader
     *
     * @param pp
     *                the proposal packet to be sent to the leader
     * @throws IOException
     */
    void writePacket(QuorumPacket pp, boolean flush) throws IOException {
        synchronized (leaderOs) {
            if (pp != null) {
                leaderOs.writeRecord(pp, "packet");
            }
            if (flush) {
                bufferedOutput.flush();
            }
        }
    }

    /**
     * read a packet from the leader
     *
     * @param pp
     *                the packet to be instantiated
     * @throws IOException
     */
    void readPacket(QuorumPacket pp) throws IOException {
        synchronized (leaderIs) {
            leaderIs.readRecord(pp, "packet"); //读取数据并反序列化
        }
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        if (pp.getType() == Leader.PING) {
            traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logQuorumPacket(LOG, traceMask, 'i', pp);
        }
    }
    
    /**
     * send a request packet to the leader
     *
     * @param request
     *                the request from the client
     * @throws IOException
     */
    void request(Request request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream oa = new DataOutputStream(baos);
        oa.writeLong(request.sessionId);
        oa.writeInt(request.cxid);
        oa.writeInt(request.type);
        if (request.request != null) {
            request.request.rewind();
            int len = request.request.remaining();
            byte b[] = new byte[len];
            request.request.get(b);
            request.request.rewind();
            oa.write(b);
        }
        oa.close();
        QuorumPacket qp = new QuorumPacket(Leader.REQUEST, -1, baos
                .toByteArray(), request.authInfo); //将请求转发给leader进行处理
        writePacket(qp, true);
    }
    
    /**
     * Returns the address of the node we think is the leader.
     */
    protected InetSocketAddress findLeader() { //找到leader
        InetSocketAddress addr = null;
        // Find the leader by id
        Vote current = self.getCurrentVote();//获取本节点的投票信息
        for (QuorumServer s : self.getView().values()) {
            if (s.id == current.getId()) { //如果当前id在集群信息中存在
                // Ensure we have the leader's correct IP address before
                // attempting to connect.
                s.recreateSocketAddresses();
                addr = s.addr; //获取集群地址
                break;
            }
        }
        if (addr == null) {
            LOG.warn("Couldn't find the leader with id = "
                    + current.getId());
        }
        return addr;
    }
    
    /**
     * Establish a connection with the Leader found by findLeader. Retries
     * 5 times before giving up. 
     * @param addr - the address of the Leader to connect to.
     * @throws IOException - if the socket connection fails on the 5th attempt
     * @throws ConnectException
     * @throws InterruptedException
     */
    protected void connectToLeader(InetSocketAddress addr) 
    throws IOException, ConnectException, InterruptedException {
        sock = new Socket();        
        sock.setSoTimeout(self.tickTime * self.initLimit); //设置连接超时时间
        for (int tries = 0; tries < 5; tries++) { //重试次数
            try {
                sock.connect(addr, self.tickTime * self.syncLimit); //连接
                sock.setTcpNoDelay(nodelay);
                break;
            } catch (IOException e) {
                if (tries == 4) {
                    LOG.error("Unexpected exception",e);
                    throw e;
                } else {
                    LOG.warn("Unexpected exception, tries="+tries+
                            ", connecting to " + addr,e);
                    sock = new Socket();
                    sock.setSoTimeout(self.tickTime * self.initLimit);
                }
            }
            Thread.sleep(1000);
        }
        leaderIs = BinaryInputArchive.getArchive(new BufferedInputStream(
                sock.getInputStream())); //设置输入流
        bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
        leaderOs = BinaryOutputArchive.getArchive(bufferedOutput);//设置输出流
    }   
    
    /**
     * Once connected to the leader, perform the handshake protocol to
     * establish a following / observing connection. 
     * @param pktType
     * @return the zxid the Leader sends for synchronization purposes.
     * @throws IOException
     */
    protected long registerWithLeader(int pktType) throws IOException{ //连接上集群之后进行注册
        /*
         * Send follower info, including last zxid and sid
         */
    	long lastLoggedZxid = self.getLastLoggedZxid();
        QuorumPacket qp = new QuorumPacket();                
        qp.setType(pktType); //设置类型为Leader.FOLLOWERINFO或者Leader.OBSERVERINFO
        qp.setZxid(ZxidUtils.makeZxid(self.getAcceptedEpoch(), 0));
        
        /*
         * Add sid to payload
         */
        LearnerInfo li = new LearnerInfo(self.getId(), 0x10000);
        ByteArrayOutputStream bsid = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(bsid);
        boa.writeRecord(li, "LearnerInfo");  //序列化LearnerInfo信息
        qp.setData(bsid.toByteArray()); //设置数据
        
        writePacket(qp, true); //发送数据包
        readPacket(qp);   //接收leader的回复(新版本是一个LEADERINFO的消息，包含leader的状态)
        final long newEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid()); //新的纪元
		if (qp.getType() == Leader.LEADERINFO) { //获取数据类型
        	// we are connected to a 1.0 server so accept the new epoch and read the next packet
        	leaderProtocolVersion = ByteBuffer.wrap(qp.getData()).getInt(); //获取协议版本号
        	byte epochBytes[] = new byte[4];
        	final ByteBuffer wrappedEpochBytes = ByteBuffer.wrap(epochBytes);
        	if (newEpoch > self.getAcceptedEpoch()) { //新纪元大于原有纪元
        		wrappedEpochBytes.putInt((int)self.getCurrentEpoch());
        		self.setAcceptedEpoch(newEpoch); //设置新纪元
        	} else if (newEpoch == self.getAcceptedEpoch()) { //如果纪元相等
        		// since we have already acked an epoch equal to the leaders, we cannot ack
        		// again, but we still need to send our lastZxid to the leader so that we can
        		// sync with it if it does assume leadership of the epoch.
        		// the -1 indicates that this reply should not count as an ack for the new epoch
                wrappedEpochBytes.putInt(-1); //回复-1不算新纪元
        	} else {
        		throw new IOException("Leaders epoch, " + newEpoch + " is less than accepted epoch, " + self.getAcceptedEpoch());
        	}
        	QuorumPacket ackNewEpoch = new QuorumPacket(Leader.ACKEPOCH, lastLoggedZxid, epochBytes, null);
        	writePacket(ackNewEpoch, true); //回复数据
            return ZxidUtils.makeZxid(newEpoch, 0); //根据新纪元得出zxid
        } else {
        	if (newEpoch > self.getAcceptedEpoch()) {
        		self.setAcceptedEpoch(newEpoch);
        	}
            if (qp.getType() != Leader.NEWLEADER) {
                LOG.error("First packet should have been NEWLEADER");
                throw new IOException("First packet should have been NEWLEADER");
            }
            return qp.getZxid();
        }
    } 
    
    /**
     * Finally, synchronize our history with the Leader. 
     * @param newLeaderZxid
     * @throws IOException
     * @throws InterruptedException
     */
    protected void syncWithLeader(long newLeaderZxid) throws IOException, InterruptedException{ //数据同步
        QuorumPacket ack = new QuorumPacket(Leader.ACK, 0, null, null); //发送ACK包
        QuorumPacket qp = new QuorumPacket();
        long newEpoch = ZxidUtils.getEpochFromZxid(newLeaderZxid); //获取纪元
        
        readPacket(qp);   //堵塞接收数据并反序列化到qp中
        LinkedList<Long> packetsCommitted = new LinkedList<Long>();
        LinkedList<PacketInFlight> packetsNotCommitted = new LinkedList<PacketInFlight>(); //还未提交的事务
        synchronized (zk) {
            if (qp.getType() == Leader.DIFF) {////接收diff，表示以diff方式与leader的数据同步 （差异化同步说明learner的zxid等于leader的zxid）
                LOG.info("Getting a diff from the leader 0x" + Long.toHexString(qp.getZxid()));                
            }
            else if (qp.getType() == Leader.SNAP) { //快照形式进行数据同步，从leader复制了一份snapshot到本地
                LOG.info("Getting a snapshot from leader");
                // The leader is going to dump the database
                // clear our own database and read
                zk.getZKDatabase().clear(); //清除内存数据库
                zk.getZKDatabase().deserializeSnapshot(leaderIs); //通过snapshot反序列化进行数据同步
                String signature = leaderIs.readString("signature"); //获取签值
                if (!signature.equals("BenWasHere")) {//验证签值
                    LOG.error("Missing signature. Got " + signature);
                    throw new IOException("Missing signature");                   
                }
            } else if (qp.getType() == Leader.TRUNC) { //触发回滚事件，回滚到leader的lastzxid
                //we need to truncate the log to the lastzxid of the leader
                LOG.warn("Truncating log to get in sync with the leader 0x"
                        + Long.toHexString(qp.getZxid()));
                boolean truncated=zk.getZKDatabase().truncateLog(qp.getZxid()); //
                if (!truncated) {
                    // not able to truncate the log
                    LOG.error("Not able to truncate the log "
                            + Long.toHexString(qp.getZxid()));
                    System.exit(13);
                }

            }
            else {
                LOG.error("Got unexpected packet from leader "
                        + qp.getType() + " exiting ... " );
                System.exit(13);

            }
            zk.getZKDatabase().setlastProcessedZxid(qp.getZxid()); //设值最后处理的zxid
            zk.createSessionTracker(); //创建session追踪器
            
            long lastQueued = 0; //上一条请求的zxid

            // in V1.0 we take a snapshot when we get the NEWLEADER message, but in pre V1.0
            // we take the snapshot at the UPDATE, since V1.0 also gets the UPDATE (after the NEWLEADER)
            // we need to make sure that we don't take the snapshot twice.
            boolean snapshotTaken = false; //是否接收到了snapshotTaken 请求
            // we are now going to start getting transactions to apply followed by an UPTODATE
            outerLoop:
            while (self.isRunning()) { //启动时数据同步,不断读取leader的数据，直到收到UPTODATE表示同步完成
                readPacket(qp); //读取数据
                switch(qp.getType()) {
                case Leader.PROPOSAL:   //接收到提议 事务提议
                    PacketInFlight pif = new PacketInFlight();
                    pif.hdr = new TxnHeader();
                    pif.rec = SerializeUtils.deserializeTxn(qp.getData(), pif.hdr); //将数据反序列化为PacketInFlight
                    if (pif.hdr.getZxid() != lastQueued + 1) { //验证zxid如果不等于最后的+1 警告
                    LOG.warn("Got zxid 0x"
                            + Long.toHexString(pif.hdr.getZxid())
                            + " expected 0x"
                            + Long.toHexString(lastQueued + 1));
                    }
                    lastQueued = pif.hdr.getZxid(); //设值lastQueued为接收到的zxid
                    packetsNotCommitted.add(pif); //将事务记录在notcommited队列中
                    break;
                case Leader.COMMIT: //收到commit请求
                    if (!snapshotTaken) { //没有进行快照
                        pif = packetsNotCommitted.peekFirst(); //从未提交队列中取出第一个
                        if (pif.hdr.getZxid() != qp.getZxid()) { //如果取出的id不等于接受到的zxid 警告并不进行提交
                            LOG.warn("Committing " + qp.getZxid() + ", but next proposal is " + pif.hdr.getZxid());
                        } else {
                            zk.processTxn(pif.hdr, pif.rec);//直接处理事务
                            packetsNotCommitted.remove();//从未提交队列中移除
                        }
                    } else {
                        packetsCommitted.add(qp.getZxid()); //
                    }
                    break;
                case Leader.INFORM://observer才会拿到INFORM消息，来同步
                    /*
                     * Only observer get this type of packet. We treat this
                     * as receiving PROPOSAL and COMMMIT.
                     */
                    PacketInFlight packet = new PacketInFlight();
                    packet.hdr = new TxnHeader();
                    packet.rec = SerializeUtils.deserializeTxn(qp.getData(), packet.hdr);
                    // Log warning message if txn comes out-of-order
                    if (packet.hdr.getZxid() != lastQueued + 1) {
                        LOG.warn("Got zxid 0x"
                                + Long.toHexString(packet.hdr.getZxid())
                                + " expected 0x"
                                + Long.toHexString(lastQueued + 1));
                    }
                    lastQueued = packet.hdr.getZxid();
                    if (!snapshotTaken) {
                        // Apply to db directly if we haven't taken the snapshot
                        zk.processTxn(packet.hdr, packet.rec);
                    } else {
                        packetsNotCommitted.add(packet);
                        packetsCommitted.add(qp.getZxid());
                    }
                    break;
                case Leader.UPTODATE: //过半机器完成了leader验证，自己也完成了数据同步,可以跳出循环
                    if (!snapshotTaken) { // true for the pre v1.0 case
                        zk.takeSnapshot();
                        self.setCurrentEpoch(newEpoch);
                    }
                    self.cnxnFactory.setZooKeeperServer(zk);                
                    break outerLoop;
                case Leader.NEWLEADER: // it will be NEWLEADER in v1.0
                    // Create updatingEpoch file and remove it after current
                    // epoch is set. QuorumPeer.loadDataBase() uses this file to
                    // detect the case where the server was terminated after
                    // taking a snapshot but before setting the current epoch.
                    File updating = new File(self.getTxnFactory().getSnapDir(),
                                        QuorumPeer.UPDATING_EPOCH_FILENAME);
                    if (!updating.exists() && !updating.createNewFile()) {
                        throw new IOException("Failed to create " +
                                              updating.toString());
                    }
                    zk.takeSnapshot();
                    self.setCurrentEpoch(newEpoch);
                    if (!updating.delete()) {
                        throw new IOException("Failed to delete " +
                                              updating.toString());
                    }
                    snapshotTaken = true;
                    writePacket(new QuorumPacket(Leader.ACK, newLeaderZxid, null, null), true);
                    break;
                }
            }
        }
        ack.setZxid(ZxidUtils.makeZxid(newEpoch, 0));
        writePacket(ack, true); //最后在发送一个ack
        sock.setSoTimeout(self.tickTime * self.syncLimit);
        zk.startup();//启动服务器
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         * 
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        self.updateElectionVote(newEpoch); ////更新选举投票，解决某个bug用的

        // We need to log the stuff that came in between the snapshot and the uptodate
        if (zk instanceof FollowerZooKeeperServer) { //如果是follower
            FollowerZooKeeperServer fzk = (FollowerZooKeeperServer)zk;
            for(PacketInFlight p: packetsNotCommitted) {
                fzk.logRequest(p.hdr, p.rec);
            }
            for(Long zxid: packetsCommitted) {
                fzk.commit(zxid);//进行commit
            }
        } else if (zk instanceof ObserverZooKeeperServer) {
            // Similar to follower, we need to log requests between the snapshot
            // and UPTODATE
            ObserverZooKeeperServer ozk = (ObserverZooKeeperServer) zk;
            for (PacketInFlight p : packetsNotCommitted) {
                Long zxid = packetsCommitted.peekFirst();
                if (p.hdr.getZxid() != zxid) {
                    // log warning message if there is no matching commit
                    // old leader send outstanding proposal to observer
                    LOG.warn("Committing " + Long.toHexString(zxid)
                            + ", but next proposal is "
                            + Long.toHexString(p.hdr.getZxid()));
                    continue;
                }
                packetsCommitted.remove();
                Request request = new Request(null, p.hdr.getClientId(),
                        p.hdr.getCxid(), p.hdr.getType(), null, null);
                request.txn = p.rec;
                request.hdr = p.hdr;
                ozk.commitRequest(request);
            }
        } else {
            // New server type need to handle in-flight packets
            throw new UnsupportedOperationException("Unknown server type");
        }
    }
    
    protected void revalidate(QuorumPacket qp) throws IOException { //验证处理
        ByteArrayInputStream bis = new ByteArrayInputStream(qp
                .getData());
        DataInputStream dis = new DataInputStream(bis);
        long sessionId = dis.readLong();
        boolean valid = dis.readBoolean();
        ServerCnxn cnxn = pendingRevalidations
        .remove(sessionId); //验证完成，从map中移除
        if (cnxn == null) {
            LOG.warn("Missing session 0x"
                    + Long.toHexString(sessionId)
                    + " for validation");
        } else {
            zk.finishSessionInit(cnxn, valid);//完成session的初始化
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                    ZooTrace.SESSION_TRACE_MASK,
                    "Session 0x" + Long.toHexString(sessionId)
                    + " is valid: " + valid);
        }
    }
        
    protected void ping(QuorumPacket qp) throws IOException {
        // Send back the ping with our session data
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        HashMap<Long, Integer> touchTable = zk
                .getTouchSnapshot(); //LearnerSessionTracker快照
        for (Entry<Long, Integer> entry : touchTable.entrySet()) {
            dos.writeLong(entry.getKey());
            dos.writeInt(entry.getValue());
        }
        qp.setData(bos.toByteArray());
        writePacket(qp, true);
    }
    
    
    /**
     * Shutdown the Peer
     */
    public void shutdown() {
        // set the zookeeper server to null
        self.cnxnFactory.setZooKeeperServer(null);
        // clear all the connections
        self.cnxnFactory.closeAll();
        // shutdown previous zookeeper
        if (zk != null) {
            zk.shutdown();
        }
    }
}
