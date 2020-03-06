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
        TxnHeader hdr; //����ͷ
        Record rec; //��¼
    }
    QuorumPeer self; //��ǰ��Ⱥ�ڵ�
    LearnerZooKeeperServer zk;//��ǰlearner״̬��zk������
    
    protected BufferedOutputStream bufferedOutput;
    
    protected Socket sock;
    
    /**
     * Socket getter
     * @return 
     */
    public Socket getSocket() {
        return sock;
    }
    
    protected InputArchive leaderIs;//����
    protected OutputArchive leaderOs;  //���
    /** the protocol version of the leader */
    protected int leaderProtocolVersion = 0x01;//Э��汾��
    
    protected static final Logger LOG = LoggerFactory.getLogger(Learner.class);

    static final private boolean nodelay = System.getProperty("follower.nodelay", "true").equals("true");
    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }   
    
    final ConcurrentHashMap<Long, ServerCnxn> pendingRevalidations =
        new ConcurrentHashMap<Long, ServerCnxn>();//client���ӵ�learnerʱ��learnerҪ��leader���REVALIDATE�������յ��ظ�֮ǰ����¼��һ��map�У���ʾ��δ���������֤
    
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
    void validateSession(ServerCnxn cnxn, long clientId, int timeout) //У��session
            throws IOException {
        LOG.info("Revalidating client: 0x" + Long.toHexString(clientId));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(clientId);//д��ͻ���id
        dos.writeInt(timeout);
        dos.close();
        QuorumPacket qp = new QuorumPacket(Leader.REVALIDATE, -1, baos
                .toByteArray(), null); //���������
        pendingRevalidations.put(clientId, cnxn);//��Ҫ��֤����δ���ؽ������¼��map��
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                                     ZooTrace.SESSION_TRACE_MASK,
                                     "To validate session 0x"
                                     + Long.toHexString(clientId));
        }
        writePacket(qp, true);//����
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
            leaderIs.readRecord(pp, "packet"); //��ȡ���ݲ������л�
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
                .toByteArray(), request.authInfo); //������ת����leader���д���
        writePacket(qp, true);
    }
    
    /**
     * Returns the address of the node we think is the leader.
     */
    protected InetSocketAddress findLeader() { //�ҵ�leader
        InetSocketAddress addr = null;
        // Find the leader by id
        Vote current = self.getCurrentVote();//��ȡ���ڵ��ͶƱ��Ϣ
        for (QuorumServer s : self.getView().values()) {
            if (s.id == current.getId()) { //�����ǰid�ڼ�Ⱥ��Ϣ�д���
                // Ensure we have the leader's correct IP address before
                // attempting to connect.
                s.recreateSocketAddresses();
                addr = s.addr; //��ȡ��Ⱥ��ַ
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
        sock.setSoTimeout(self.tickTime * self.initLimit); //�������ӳ�ʱʱ��
        for (int tries = 0; tries < 5; tries++) { //���Դ���
            try {
                sock.connect(addr, self.tickTime * self.syncLimit); //����
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
                sock.getInputStream())); //����������
        bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
        leaderOs = BinaryOutputArchive.getArchive(bufferedOutput);//���������
    }   
    
    /**
     * Once connected to the leader, perform the handshake protocol to
     * establish a following / observing connection. 
     * @param pktType
     * @return the zxid the Leader sends for synchronization purposes.
     * @throws IOException
     */
    protected long registerWithLeader(int pktType) throws IOException{ //�����ϼ�Ⱥ֮�����ע��
        /*
         * Send follower info, including last zxid and sid
         */
    	long lastLoggedZxid = self.getLastLoggedZxid();
        QuorumPacket qp = new QuorumPacket();                
        qp.setType(pktType); //��������ΪLeader.FOLLOWERINFO����Leader.OBSERVERINFO
        qp.setZxid(ZxidUtils.makeZxid(self.getAcceptedEpoch(), 0));
        
        /*
         * Add sid to payload
         */
        LearnerInfo li = new LearnerInfo(self.getId(), 0x10000);
        ByteArrayOutputStream bsid = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(bsid);
        boa.writeRecord(li, "LearnerInfo");  //���л�LearnerInfo��Ϣ
        qp.setData(bsid.toByteArray()); //��������
        
        writePacket(qp, true); //�������ݰ�
        readPacket(qp);   //����leader�Ļظ�(�°汾��һ��LEADERINFO����Ϣ������leader��״̬)
        final long newEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid()); //�µļ�Ԫ
		if (qp.getType() == Leader.LEADERINFO) { //��ȡ��������
        	// we are connected to a 1.0 server so accept the new epoch and read the next packet
        	leaderProtocolVersion = ByteBuffer.wrap(qp.getData()).getInt(); //��ȡЭ��汾��
        	byte epochBytes[] = new byte[4];
        	final ByteBuffer wrappedEpochBytes = ByteBuffer.wrap(epochBytes);
        	if (newEpoch > self.getAcceptedEpoch()) { //�¼�Ԫ����ԭ�м�Ԫ
        		wrappedEpochBytes.putInt((int)self.getCurrentEpoch());
        		self.setAcceptedEpoch(newEpoch); //�����¼�Ԫ
        	} else if (newEpoch == self.getAcceptedEpoch()) { //�����Ԫ���
        		// since we have already acked an epoch equal to the leaders, we cannot ack
        		// again, but we still need to send our lastZxid to the leader so that we can
        		// sync with it if it does assume leadership of the epoch.
        		// the -1 indicates that this reply should not count as an ack for the new epoch
                wrappedEpochBytes.putInt(-1); //�ظ�-1�����¼�Ԫ
        	} else {
        		throw new IOException("Leaders epoch, " + newEpoch + " is less than accepted epoch, " + self.getAcceptedEpoch());
        	}
        	QuorumPacket ackNewEpoch = new QuorumPacket(Leader.ACKEPOCH, lastLoggedZxid, epochBytes, null);
        	writePacket(ackNewEpoch, true); //�ظ�����
            return ZxidUtils.makeZxid(newEpoch, 0); //�����¼�Ԫ�ó�zxid
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
    protected void syncWithLeader(long newLeaderZxid) throws IOException, InterruptedException{ //����ͬ��
        QuorumPacket ack = new QuorumPacket(Leader.ACK, 0, null, null); //����ACK��
        QuorumPacket qp = new QuorumPacket();
        long newEpoch = ZxidUtils.getEpochFromZxid(newLeaderZxid); //��ȡ��Ԫ
        
        readPacket(qp);   //�����������ݲ������л���qp��
        LinkedList<Long> packetsCommitted = new LinkedList<Long>();
        LinkedList<PacketInFlight> packetsNotCommitted = new LinkedList<PacketInFlight>(); //��δ�ύ������
        synchronized (zk) {
            if (qp.getType() == Leader.DIFF) {////����diff����ʾ��diff��ʽ��leader������ͬ�� �����컯ͬ��˵��learner��zxid����leader��zxid��
                LOG.info("Getting a diff from the leader 0x" + Long.toHexString(qp.getZxid()));                
            }
            else if (qp.getType() == Leader.SNAP) { //������ʽ��������ͬ������leader������һ��snapshot������
                LOG.info("Getting a snapshot from leader");
                // The leader is going to dump the database
                // clear our own database and read
                zk.getZKDatabase().clear(); //����ڴ����ݿ�
                zk.getZKDatabase().deserializeSnapshot(leaderIs); //ͨ��snapshot�����л���������ͬ��
                String signature = leaderIs.readString("signature"); //��ȡǩֵ
                if (!signature.equals("BenWasHere")) {//��֤ǩֵ
                    LOG.error("Missing signature. Got " + signature);
                    throw new IOException("Missing signature");                   
                }
            } else if (qp.getType() == Leader.TRUNC) { //�����ع��¼����ع���leader��lastzxid
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
            zk.getZKDatabase().setlastProcessedZxid(qp.getZxid()); //��ֵ������zxid
            zk.createSessionTracker(); //����session׷����
            
            long lastQueued = 0; //��һ�������zxid

            // in V1.0 we take a snapshot when we get the NEWLEADER message, but in pre V1.0
            // we take the snapshot at the UPDATE, since V1.0 also gets the UPDATE (after the NEWLEADER)
            // we need to make sure that we don't take the snapshot twice.
            boolean snapshotTaken = false; //�Ƿ���յ���snapshotTaken ����
            // we are now going to start getting transactions to apply followed by an UPTODATE
            outerLoop:
            while (self.isRunning()) { //����ʱ����ͬ��,���϶�ȡleader�����ݣ�ֱ���յ�UPTODATE��ʾͬ�����
                readPacket(qp); //��ȡ����
                switch(qp.getType()) {
                case Leader.PROPOSAL:   //���յ����� ��������
                    PacketInFlight pif = new PacketInFlight();
                    pif.hdr = new TxnHeader();
                    pif.rec = SerializeUtils.deserializeTxn(qp.getData(), pif.hdr); //�����ݷ����л�ΪPacketInFlight
                    if (pif.hdr.getZxid() != lastQueued + 1) { //��֤zxid�������������+1 ����
                    LOG.warn("Got zxid 0x"
                            + Long.toHexString(pif.hdr.getZxid())
                            + " expected 0x"
                            + Long.toHexString(lastQueued + 1));
                    }
                    lastQueued = pif.hdr.getZxid(); //��ֵlastQueuedΪ���յ���zxid
                    packetsNotCommitted.add(pif); //�������¼��notcommited������
                    break;
                case Leader.COMMIT: //�յ�commit����
                    if (!snapshotTaken) { //û�н��п���
                        pif = packetsNotCommitted.peekFirst(); //��δ�ύ������ȡ����һ��
                        if (pif.hdr.getZxid() != qp.getZxid()) { //���ȡ����id�����ڽ��ܵ���zxid ���沢�������ύ
                            LOG.warn("Committing " + qp.getZxid() + ", but next proposal is " + pif.hdr.getZxid());
                        } else {
                            zk.processTxn(pif.hdr, pif.rec);//ֱ�Ӵ�������
                            packetsNotCommitted.remove();//��δ�ύ�������Ƴ�
                        }
                    } else {
                        packetsCommitted.add(qp.getZxid()); //
                    }
                    break;
                case Leader.INFORM://observer�Ż��õ�INFORM��Ϣ����ͬ��
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
                case Leader.UPTODATE: //������������leader��֤���Լ�Ҳ���������ͬ��,��������ѭ��
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
        writePacket(ack, true); //����ڷ���һ��ack
        sock.setSoTimeout(self.tickTime * self.syncLimit);
        zk.startup();//����������
        /*
         * Update the election vote here to ensure that all members of the
         * ensemble report the same vote to new servers that start up and
         * send leader election notifications to the ensemble.
         * 
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        self.updateElectionVote(newEpoch); ////����ѡ��ͶƱ�����ĳ��bug�õ�

        // We need to log the stuff that came in between the snapshot and the uptodate
        if (zk instanceof FollowerZooKeeperServer) { //�����follower
            FollowerZooKeeperServer fzk = (FollowerZooKeeperServer)zk;
            for(PacketInFlight p: packetsNotCommitted) {
                fzk.logRequest(p.hdr, p.rec);
            }
            for(Long zxid: packetsCommitted) {
                fzk.commit(zxid);//����commit
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
    
    protected void revalidate(QuorumPacket qp) throws IOException { //��֤����
        ByteArrayInputStream bis = new ByteArrayInputStream(qp
                .getData());
        DataInputStream dis = new DataInputStream(bis);
        long sessionId = dis.readLong();
        boolean valid = dis.readBoolean();
        ServerCnxn cnxn = pendingRevalidations
        .remove(sessionId); //��֤��ɣ���map���Ƴ�
        if (cnxn == null) {
            LOG.warn("Missing session 0x"
                    + Long.toHexString(sessionId)
                    + " for validation");
        } else {
            zk.finishSessionInit(cnxn, valid);//���session�ĳ�ʼ��
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
                .getTouchSnapshot(); //LearnerSessionTracker����
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
