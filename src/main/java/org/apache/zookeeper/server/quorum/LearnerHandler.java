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
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * There will be an instance of this class created by the Leader for each
 * learner. All communication with a learner is handled by this
 * class.
 */
public class LearnerHandler extends ZooKeeperThread {
    private static final Logger LOG = LoggerFactory.getLogger(LearnerHandler.class);

    protected final Socket sock;    

    public Socket getSocket() {
        return sock;
    }

    final Leader leader; //leader的角色

    /** Deadline for receiving the next ack. If we are bootstrapping then
     * it's based on the initLimit, if we are done bootstrapping it's based
     * on the syncLimit. Once the deadline is past this learner should
     * be considered no longer "sync'd" with the leader. */
    volatile long tickOfNextAckDeadline;//下一个接收ack的deadline，启动时(数据同步)是一个标准，完成启动后(正常交互)，是另一个标准
    
    /**
     * ZooKeeper server identifier of this learner
     */
    protected long sid = 0; //当前这个learner的myid
    
    long getSid(){
        return sid;
    }                    

    protected int version = 0x1; //当前learner版本号
    
    int getVersion() {
    	return version;
    }
    
    /**
     * The packets to be sent to the learner
     */
    final LinkedBlockingQueue<QuorumPacket> queuedPackets =
        new LinkedBlockingQueue<QuorumPacket>(); //待发送给learner的请求数据

    /**
     * This class controls the time that the Leader has been
     * waiting for acknowledgement of a proposal from this Learner.
     * If the time is above syncLimit, the connection will be closed.
     * It keeps track of only one proposal at a time, when the ACK for
     * that proposal arrives, it switches to the last proposal received
     * or clears the value if there is no pending proposal.
     */
    private class SyncLimitCheck {
        private boolean started = false;
        private long currentZxid = 0;//当前更新了但是没有收到ack的proposal的zxid
        private long currentTime = 0;//当前更新了但是没有收到ack的proposal的时间
        private long nextZxid = 0;//下一次更新了但是没有收到ack的proposal的zxid
        private long nextTime = 0;//下一次更新了但是没有收到ack的proposal的时间

        public synchronized void start() { //启动同步检测机制
            started = true;
        }

        public synchronized void updateProposal(long zxid, long time) { //发送proposal时，更新提议的统计时间
            if (!started) {
                return;
            }
            if (currentTime == 0) {
                currentTime = time;
                currentZxid = zxid;
            } else {
                nextTime = time;
                nextZxid = zxid;
            }
        }

        public synchronized void updateAck(long zxid) { //升级ACK
             if (currentZxid == zxid) { //如果当前的提案已经收到了确认请求 将当前的设置为下一条的
                 currentTime = nextTime;
                 currentZxid = nextZxid;
                 nextTime = 0;
                 nextZxid = 0;
             } else if (nextZxid == zxid) {
                 LOG.warn("ACK for " + zxid + " received before ACK for " + currentZxid + "!!!!");
                 nextTime = 0;
                 nextZxid = 0;
             }
        }

        public synchronized boolean check(long time) { //如果等待时间超时了返回true
            if (currentTime == 0) {
                return true;
            } else {
                long msDelay = (time - currentTime) / 1000000;
                return (msDelay < (leader.self.tickTime * leader.self.syncLimit));
            }
        }
    };

    private SyncLimitCheck syncLimitCheck = new SyncLimitCheck();

    private BinaryInputArchive ia; //

    private BinaryOutputArchive oa; //

    private BufferedOutputStream bufferedOutput;

    LearnerHandler(Socket sock, Leader leader) throws IOException { //构造方法当有learner连接上来的时候创建该类
        super("LearnerHandler-" + sock.getRemoteSocketAddress());
        this.sock = sock;
        this.leader = leader;
        leader.addLearnerHandler(this); //将learnerHandler记录下来 leader和learnerHandler是一对多的关系
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LearnerHandler ").append(sock);
        sb.append(" tickOfNextAckDeadline:").append(tickOfNextAckDeadline());
        sb.append(" synced?:").append(synced());
        sb.append(" queuedPacketLength:").append(queuedPackets.size());
        return sb.toString();
    }

    /**
     * If this packet is queued, the sender thread will exit
     */
    final QuorumPacket proposalOfDeath = new QuorumPacket(); //关闭当前线程最后的一个包

    private LearnerType  learnerType = LearnerType.PARTICIPANT; //默认的learner类型（也叫Follower）,也可以设置为OBSERVER
    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * This method will use the thread to send packets added to the
     * queuedPackets list
     *
     * @throws InterruptedException
     */
    private void sendPackets() throws InterruptedException { //发送数据包给learner
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK; //跟踪标志设置
        while (true) {
            try {
                QuorumPacket p;
                p = queuedPackets.poll(); //从发送队列中获取一条数据
                if (p == null) {//如果为空
                    bufferedOutput.flush();
                    p = queuedPackets.take(); //堵塞获取
                }

                if (p == proposalOfDeath) { //待发送的包如果是最后一个包直接break
                    // Packet of death!
                    break;
                }
                if (p.getType() == Leader.PING) { //
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK; //
                }
                if (p.getType() == Leader.PROPOSAL) { //如果是议案
                    syncLimitCheck.updateProposal(p.getZxid(), System.nanoTime()); //更新议案信息
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'o', p);
                }
                oa.writeRecord(p, "packet"); //写入数据包给learner
            } catch (IOException e) {
                if (!sock.isClosed()) {
                    LOG.warn("Unexpected exception at " + this, e);
                    try {
                        // this will cause everything to shutdown on
                        // this learner handler and will help notify
                        // the learner/observer instantaneously
                        sock.close();
                    } catch(IOException ie) {
                        LOG.warn("Error closing socket for handler " + this, ie);
                    }
                }
                break;
            }
        }
    }

    static public String packetToString(QuorumPacket p) {
        String type = null;
        String mess = null;
        Record txn = null;
        
        switch (p.getType()) {
        case Leader.ACK:
            type = "ACK";
            break;
        case Leader.COMMIT:
            type = "COMMIT";
            break;
        case Leader.FOLLOWERINFO:
            type = "FOLLOWERINFO";
            break;    
        case Leader.NEWLEADER:
            type = "NEWLEADER";
            break;
        case Leader.PING:
            type = "PING";
            break;
        case Leader.PROPOSAL:
            type = "PROPOSAL";
            TxnHeader hdr = new TxnHeader();
            try {
                txn = SerializeUtils.deserializeTxn(p.getData(), hdr);
                // mess = "transaction: " + txn.toString();
            } catch (IOException e) {
                LOG.warn("Unexpected exception",e);
            }
            break;
        case Leader.REQUEST:
            type = "REQUEST";
            break;
        case Leader.REVALIDATE:
            type = "REVALIDATE";
            ByteArrayInputStream bis = new ByteArrayInputStream(p.getData());
            DataInputStream dis = new DataInputStream(bis);
            try {
                long id = dis.readLong();
                mess = " sessionid = " + id;
            } catch (IOException e) {
                LOG.warn("Unexpected exception", e);
            }

            break;
        case Leader.UPTODATE:
            type = "UPTODATE";
            break;
        default:
            type = "UNKNOWN" + p.getType();
        }
        String entry = null;
        if (type != null) {
            entry = type + " " + Long.toHexString(p.getZxid()) + " " + mess;
        }
        return entry;
    }

    /**
     * This thread will receive packets from the peer and process them and
     * also listen to new connections from new peers.
     */
    @Override
    public void run() {
        try {
            tickOfNextAckDeadline = leader.self.tick
                    + leader.self.initLimit + leader.self.syncLimit; ////初始化，是leader当前周期(leader.self.tick) 再加上初始化以及同步的limit(initLimit + syncLimit)

            ia = BinaryInputArchive.getArchive(new BufferedInputStream(sock
                    .getInputStream())); //获取输入流
            bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput); //获取输出流

            QuorumPacket qp = new QuorumPacket(); //
            ia.readRecord(qp, "packet"); //读取数据包
            if(qp.getType() != Leader.FOLLOWERINFO && qp.getType() != Leader.OBSERVERINFO){
            	LOG.error("First packet " + qp.toString()
                        + " is not FOLLOWERINFO or OBSERVERINFO!");
                return;
            }
            byte learnerInfoData[] = qp.getData(); //接收learner发送过来的LearnerInfo,包含sid
            if (learnerInfoData != null) {
            	if (learnerInfoData.length == 8) {//
            		ByteBuffer bbsid = ByteBuffer.wrap(learnerInfoData);
            		this.sid = bbsid.getLong(); //设置sid
            	} else {
            		LearnerInfo li = new LearnerInfo();
            		ByteBufferInputStream.byteBuffer2Record(ByteBuffer.wrap(learnerInfoData), li);
            		this.sid = li.getServerid(); //设置sid
            		this.version = li.getProtocolVersion(); //设置版本号
            	}
            } else {
            	this.sid = leader.followerCounter.getAndDecrement(); //跟随者数目加1
            }

            LOG.info("Follower sid: " + sid + " : info : "
                    + leader.self.quorumPeers.get(sid));
                        
            if (qp.getType() == Leader.OBSERVERINFO) { //设置类型默认为PARTICIPANT 如果是observer设置为对应的observer
                  learnerType = LearnerType.OBSERVER;
            }            
            
            long lastAcceptedEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid()); //设置当前learner的最新的epoch
            
            long peerLastZxid;//
            StateSummary ss = null;
            long zxid = qp.getZxid(); //设置zxid
            long newEpoch = leader.getEpochToPropose(this.getSid(), lastAcceptedEpoch);//
            
            if (this.getVersion() < 0x10000) {//leader是旧版本
                // we are going to have to extrapolate the epoch information
                long epoch = ZxidUtils.getEpochFromZxid(zxid);
                ss = new StateSummary(epoch, zxid);
                // fake the message
                leader.waitForEpochAck(this.getSid(), ss);
            } else { //新版本处理
                byte ver[] = new byte[4];
                ByteBuffer.wrap(ver).putInt(0x10000);
                QuorumPacket newEpochPacket = new QuorumPacket(Leader.LEADERINFO, ZxidUtils.makeZxid(newEpoch, 0), ver, null);
                oa.writeRecord(newEpochPacket, "packet"); //leaderinfo数据
                bufferedOutput.flush();
                QuorumPacket ackEpochPacket = new QuorumPacket();
                ia.readRecord(ackEpochPacket, "packet"); //接收learner返回的ack包
                if (ackEpochPacket.getType() != Leader.ACKEPOCH) {
                    LOG.error(ackEpochPacket.toString()
                            + " is not ACKEPOCH");
                    return;
				}
                ByteBuffer bbepoch = ByteBuffer.wrap(ackEpochPacket.getData()); //获取数据
                ss = new StateSummary(bbepoch.getInt(), ackEpochPacket.getZxid());
                leader.waitForEpochAck(this.getSid(), ss);
            }
            peerLastZxid = ss.getLastZxid(); //设置最后的zxid
            
            /* the default to send to the follower */
            int packetToSend = Leader.SNAP;//默认使用SNAP 全量同步
            long zxidToSend = 0;
            long leaderLastZxid = 0;
            /** the packets that the follower needs to get updates from **/
            long updates = peerLastZxid;
            
            /* we are sending the diff check if we have proposals in memory to be able to 
             * send a diff to the 
             */ 
            ReentrantReadWriteLock lock = leader.zk.getZKDatabase().getLogLock();
            ReadLock rl = lock.readLock(); //获取读锁
            try {
                rl.lock();        
                final long maxCommittedLog = leader.zk.getZKDatabase().getmaxCommittedLog(); //内存中记录的最大日志zxid
                final long minCommittedLog = leader.zk.getZKDatabase().getminCommittedLog();//内存中记录的最小日志zxid
                LOG.info("Synchronizing with Follower sid: " + sid
                        +" maxCommittedLog=0x"+Long.toHexString(maxCommittedLog)
                        +" minCommittedLog=0x"+Long.toHexString(minCommittedLog)
                        +" peerLastZxid=0x"+Long.toHexString(peerLastZxid));

                LinkedList<Proposal> proposals = leader.zk.getZKDatabase().getCommittedLog(); //获取议案列表

                if (peerLastZxid == leader.zk.getZKDatabase().getDataTreeLastProcessedZxid()) { //如果最后的zxid等于内存数据库中最后的zxid说明同步的差不多了
                    // Follower is already sync with us, send empty diff
                    LOG.info("leader and follower are in sync, zxid=0x{}",
                            Long.toHexString(peerLastZxid));
                    packetToSend = Leader.DIFF; //使用DIFF模式进行发送 说明没有
                    zxidToSend = peerLastZxid;//如果learner已经是同步的了，也发送DIFF，只是发送的zxidToSend和learner本地一样，相当于空的DIFF
                } else if (proposals.size() != 0) { //
                    LOG.debug("proposal size is {}", proposals.size());
                    if ((maxCommittedLog >= peerLastZxid) //最大的提交大于最后的zxid minCommittedLog<=peerLastZxid<=maxCommittedLog  DIFF增量同步
                            && (minCommittedLog <= peerLastZxid)) { //最小的提交小于zxid
                        LOG.debug("Sending proposals to follower");

                        // as we look through proposals, this variable keeps track of previous
                        // proposal Id.
                        long prevProposalZxid = minCommittedLog; //获取最小的zxid

                        // Keep track of whether we are about to send the first packet.
                        // Before sending the first packet, we have to tell the learner
                        // whether to expect a trunc or a diff
                        boolean firstPacket=true;

                        // If we are here, we can use committedLog to sync with
                        // follower. Then we only need to decide whether to
                        // send trunc or not
                        packetToSend = Leader.DIFF;
                        zxidToSend = maxCommittedLog; //最大的zxid

                        for (Proposal propose: proposals) { //将提议进行提交
                            // skip the proposals the peer already has
                            if (propose.packet.getZxid() <= peerLastZxid) { //如果议案中的zxid小于最后的zxid 说明被处理过了
                                prevProposalZxid = propose.packet.getZxid();//设置prevProposalZxid为当前议案的zxid
                                continue;
                            } else {
                                // If we are sending the first packet, figure out whether to trunc
                                // in case the follower has some proposals that the leader doesn't
                                if (firstPacket) { //第一个发送的packet
                                    firstPacket = false;
                                    // Does the peer have some proposals that the leader hasn't seen yet
                                    if (prevProposalZxid < peerLastZxid) { //如果learner有一些leader不知道的请求(正常来说应该是prevProposalZxid == peerLastZxid)
                                        // send a trunc message before sending the diff
                                        packetToSend = Leader.TRUNC;    //回滚请求至指定的zxid
                                        zxidToSend = prevProposalZxid;
                                        updates = zxidToSend;
                                    }
                                }
                                queuePacket(propose.packet); //数据包加入队列议案
                                QuorumPacket qcommit = new QuorumPacket(Leader.COMMIT, propose.packet.getZxid(),
                                        null, null); //数据提交包
                                queuePacket(qcommit);//数据包加入队列
                            }
                        }
                    } else if (peerLastZxid > maxCommittedLog) {//如果最后的zxid大于内存中最大的证明 follower 上有些提议 proposal 并未在 leader 上提交，回滚
                        LOG.debug("Sending TRUNC to follower zxidToSend=0x{} updates=0x{}",
                                Long.toHexString(maxCommittedLog),
                                Long.toHexString(updates));

                        packetToSend = Leader.TRUNC; //回滚
                        zxidToSend = maxCommittedLog;
                        updates = zxidToSend;
                    } else {
                        LOG.warn("Unhandled proposal scenario");
                    }
                } else {
                    // just let the state transfer happen
                    LOG.debug("proposals is empty");
                }               

                LOG.info("Sending " + Leader.getPacketType(packetToSend));
                leaderLastZxid = leader.startForwarding(this, updates);

            } finally {
                rl.unlock();
            }

             QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER,
                    ZxidUtils.makeZxid(newEpoch, 0), null, null); //生成NEWLEADER的packet,发给learner代表自己需要同步的信息发完了
             if (getVersion() < 0x10000) { //
                oa.writeRecord(newLeaderQP, "packet");
            } else {
                queuedPackets.add(newLeaderQP); //加入消息至队列
            }
            bufferedOutput.flush();
            //Need to set the zxidToSend to the latest zxid
            if (packetToSend == Leader.SNAP) { //如果发出snap，代表告知learner进行snap方式的数据同步
                zxidToSend = leader.zk.getZKDatabase().getDataTreeLastProcessedZxid();
            }
            oa.writeRecord(new QuorumPacket(packetToSend, zxidToSend, null, null), "packet"); //写入序列化数据
            bufferedOutput.flush();
            
            /* if we are not truncating or sending a diff just send a snapshot */
            if (packetToSend == Leader.SNAP) {
                LOG.info("Sending snapshot last zxid of peer is 0x"
                        + Long.toHexString(peerLastZxid) + " " 
                        + " zxid of leader is 0x"
                        + Long.toHexString(leaderLastZxid)
                        + "sent zxid of db as 0x" 
                        + Long.toHexString(zxidToSend));
                // Dump data to peer
                leader.zk.getZKDatabase().serializeSnapshot(oa);
                oa.writeString("BenWasHere", "signature");//写入签值
            }
            bufferedOutput.flush();
            
            // Start sending packets
            new Thread() {
                public void run() {
                    Thread.currentThread().setName(
                            "Sender-" + sock.getRemoteSocketAddress());
                    try {
                        sendPackets();//不断发送packets直到接受到proposalOfDeath
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected interruption",e);
                    }
                }
            }.start();
            
            /*
             * Have to wait for the first ACK, wait until 
             * the leader is ready, and only then we can
             * start processing messages.
             */
            qp = new QuorumPacket(); //
            ia.readRecord(qp, "packet"); //Learner接收到NEWLEADER 一定会返回ACK
            if(qp.getType() != Leader.ACK){
                LOG.error("Next packet was supposed to be an ACK");
                return;
            }
            LOG.info("Received NEWLEADER-ACK message from " + getSid());
            leader.waitForNewLeaderAck(getSid(), qp.getZxid(), getLearnerType());

            syncLimitCheck.start(); //开始同步检测
            
            // now that the ack has been processed expect the syncLimit
            sock.setSoTimeout(leader.self.tickTime * leader.self.syncLimit); //设置超时

            /*
             * Wait until leader starts up
             */
            synchronized(leader.zk){
                while(!leader.zk.isRunning() && !this.isInterrupted()){
                    leader.zk.wait(20);
                }
            }
            // Mutation packets will be queued during the serialize,
            // so we need to mark when the peer can actually start
            // using the data
            //
            queuedPackets.add(new QuorumPacket(Leader.UPTODATE, -1, null, null)); //添加同步完成包

            while (true) {//正常交互，处理learner的请求等
                qp = new QuorumPacket();
                ia.readRecord(qp, "packet"); //读取数据包堵塞

                long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
                if (qp.getType() == Leader.PING) { //如果是ping请求
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'i', qp);
                }
                tickOfNextAckDeadline = leader.self.tick + leader.self.syncLimit;


                ByteBuffer bb;
                long sessionId;
                int cxid;
                int type;

                switch (qp.getType()) { //根据不同的请求发送回复
                case Leader.ACK:
                    if (this.learnerType == LearnerType.OBSERVER) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Received ACK from Observer  " + this.sid);
                        }
                    }
                    syncLimitCheck.updateAck(qp.getZxid());//更新proposal对应的ack时间
                    leader.processAck(this.sid, qp.getZxid(), sock.getLocalSocketAddress());
                    break;
                case Leader.PING:
                    // Process the touches
                    ByteArrayInputStream bis = new ByteArrayInputStream(qp
                            .getData());
                    DataInputStream dis = new DataInputStream(bis);
                    while (dis.available() > 0) {
                        long sess = dis.readLong();
                        int to = dis.readInt();
                        leader.zk.touch(sess, to);
                    }
                    break;
                case Leader.REVALIDATE:
                    bis = new ByteArrayInputStream(qp.getData());
                    dis = new DataInputStream(bis);
                    long id = dis.readLong();
                    int to = dis.readInt();
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(bos);
                    dos.writeLong(id);
                    boolean valid = leader.zk.touch(id, to);
                    if (valid) {
                        try {
                            //set the session owner
                            // as the follower that
                            // owns the session
                            leader.zk.setOwner(id, this);
                        } catch (SessionExpiredException e) {
                            LOG.error("Somehow session " + Long.toHexString(id) + " expired right after being renewed! (impossible)", e);
                        }
                    }
                    if (LOG.isTraceEnabled()) {
                        ZooTrace.logTraceMessage(LOG,
                                                 ZooTrace.SESSION_TRACE_MASK,
                                                 "Session 0x" + Long.toHexString(id)
                                                 + " is valid: "+ valid);
                    }
                    dos.writeBoolean(valid);
                    qp.setData(bos.toByteArray());
                    queuedPackets.add(qp);
                    break;
                case Leader.REQUEST:     //请求
                    bb = ByteBuffer.wrap(qp.getData());
                    sessionId = bb.getLong();
                    cxid = bb.getInt();
                    type = bb.getInt();
                    bb = bb.slice();
                    Request si;
                    if(type == OpCode.sync){
                        si = new LearnerSyncRequest(this, sessionId, cxid, type, bb, qp.getAuthinfo());
                    } else {
                        si = new Request(null, sessionId, cxid, type, bb, qp.getAuthinfo());
                    }
                    si.setOwner(this);
                    leader.zk.submitRequest(si); //提交请求
                    break;
                default:
                    LOG.warn("unexpected quorum packet, type: {}", packetToString(qp));
                    break;
                }
            }
        } catch (IOException e) {
            if (sock != null && !sock.isClosed()) {
                LOG.error("Unexpected exception causing shutdown while sock "
                        + "still open", e);
            	//close the socket to make sure the 
            	//other side can see it being close
            	try {
            		sock.close();
            	} catch(IOException ie) {
            		// do nothing
            	}
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected exception causing shutdown", e);
        } finally {
            LOG.warn("******* GOODBYE " 
                    + (sock != null ? sock.getRemoteSocketAddress() : "<null>")
                    + " ********");
            shutdown();
        }
    }

    public void shutdown() { //验证关闭
        // Send the packet of death
        try {
            queuedPackets.put(proposalOfDeath);//设置队列中最后一个包为关闭的数据包
        } catch (InterruptedException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }
        try {
            if (sock != null && !sock.isClosed()) {
                sock.close();
            }
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during socket close", e);
        }
        this.interrupt(); //终端线程
        leader.removeLearnerHandler(this); //从leader的learnerHandler中移除该条记录
    }

    public long tickOfNextAckDeadline() {
        return tickOfNextAckDeadline;
    }

    /**
     * ping calls from the leader to the peers
     */
    public void ping() { //发送ping给集群leader
        long id;
        if (syncLimitCheck.check(System.nanoTime())) { //检查是否超时 如果还没有超时
            synchronized(leader) {
                id = leader.lastProposed; //设置的zxid??
            }
            QuorumPacket ping = new QuorumPacket(Leader.PING, id, null, null);
            queuePacket(ping); //添加ping消息至发送队列
        } else { //如果已经超时，就关闭这个handler
            LOG.warn("Closing connection to peer due to transaction timeout.");
            shutdown();//关闭当前handler，sock，以及让syncLimitCheck任务最终停止
        }
    }

    void queuePacket(QuorumPacket p) {
        queuedPackets.add(p);
    }

    public boolean synced() { //是否同步
        return isAlive()
        && leader.self.tick <= tickOfNextAckDeadline;
    }
}
