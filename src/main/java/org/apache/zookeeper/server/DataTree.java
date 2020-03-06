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

package org.apache.zookeeper.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jute.Index;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.PathTrie;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.txn.CheckVersionTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.SetACLTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.Txn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class maintains the tree data structure. It doesn't have any networking
 * or client connection code in it so that it can be tested in a stand alone
 * way.
 * <p>
 * The tree maintains two parallel data structures: a hashtable that maps from
 * full paths to DataNodes and a tree of DataNodes. All accesses to a path is
 * through the hashtable. The tree is traversed only when serializing to disk.
 */
public class DataTree {
    private static final Logger LOG = LoggerFactory.getLogger(DataTree.class);

    /**
     * This hashtable provides a fast lookup to the datanodes. The tree is the
     * source of truth and is where all the locking occurs
     */
    private final ConcurrentHashMap<String, DataNode> nodes =
        new ConcurrentHashMap<String, DataNode>();   //���еĽڵ���Ϣpath��DataNode��ӳ��

    private final WatchManager dataWatches = new WatchManager(); //���ݹ۲��߹���

    private final WatchManager childWatches = new WatchManager();//���ӽڵ�۲��߹���

    /** the root of zookeeper tree */
    private static final String rootZookeeper = "/"; //���ڵ�

    /** the zookeeper nodes that acts as the management and status node **/
    private static final String procZookeeper = Quotas.procZookeeper; //zookeeper�ڵ�

    /** this will be the string thats stored as a child of root */
    private static final String procChildZookeeper = procZookeeper.substring(1); //ȥ��"/"��zookeeper�ڵ��ַ���

    /**
     * the zookeeper quota node that acts as the quota management node for
     * zookeeper
     */
    private static final String quotaZookeeper = Quotas.quotaZookeeper; //���Ŀ¼

    /** this will be the string thats stored as a child of /zookeeper */
    private static final String quotaChildZookeeper = quotaZookeeper
            .substring(procZookeeper.length() + 1); //ȥ��"/zookeeper/"��quota�ڵ��ַ���

    /**
     * the path trie that keeps track fo the quota nodes in this datatree
     */
    private final PathTrie pTrie = new PathTrie(); //�ֵ���

    /**
     * This hashtable lists the paths of the ephemeral nodes of a session.
     */
    private final Map<Long, HashSet<String>> ephemerals =
        new ConcurrentHashMap<Long, HashSet<String>>(); //���е���ʱ�ڵ�

    /**
     * this is map from longs to acl's. It saves acl's being stored for each
     * datanode.
     */
    public final Map<Long, List<ACL>> longKeyMap =
        new HashMap<Long, List<ACL>>(); //ACL��Ϣkey֮Ϊһ��long����ţ�long��ֵ�洢��DataNode��

    /**
     * this a map from acls to long.
     */
    public final Map<List<ACL>, Long> aclKeyMap =
        new HashMap<List<ACL>, Long>(); //ACL��Ϣ ������������෴keyֵΪACL��Ϣ

    /**
     * these are the number of acls that we have in the datatree
     */
    protected long aclIndex = 0; //ACL����

    @SuppressWarnings("unchecked")
    public HashSet<String> getEphemerals(long sessionId) {
        HashSet<String> retv = ephemerals.get(sessionId);
        if (retv == null) {
            return new HashSet<String>();
        }
        HashSet<String> cloned = null;
        synchronized (retv) {
            cloned = (HashSet<String>) retv.clone();
        }
        return cloned;
    }

    public Map<Long, HashSet<String>> getEphemeralsMap() {
        return ephemerals;
    }

    private long incrementIndex() {
        return ++aclIndex;
    }

    /**
     * compare two list of acls. if there elements are in the same order and the
     * same size then return true else return false
     *
     * @param lista
     *            the list to be compared
     * @param listb
     *            the list to be compared
     * @return true if and only if the lists are of the same size and the
     *         elements are in the same order in lista and listb
     */
    private boolean listACLEquals(List<ACL> lista, List<ACL> listb) {
        if (lista.size() != listb.size()) {
            return false;
        }
        for (int i = 0; i < lista.size(); i++) {
            ACL a = lista.get(i);
            ACL b = listb.get(i);
            if (!a.equals(b)) {
                return false;
            }
        }
        return true;
    }

    /**
     * converts the list of acls to a list of longs.
     *
     * @param acls
     * @return a list of longs that map to the acls
     */
    public synchronized Long convertAcls(List<ACL> acls) { //���ACL��Ϣ
        if (acls == null) //���ACLΪ�շ���-1
            return -1L;
        // get the value from the map
        Long ret = aclKeyMap.get(acls); //��ȡACL�������һ����ACL��Ϣ�͸��ã�û�б�Ҫ�����´���һ��
        // could not find the map
        if (ret != null)//�����Ų�Ϊ��ֱ�ӷ���
            return ret;
        long val = incrementIndex(); //��ż�1
        longKeyMap.put(val, acls);//���ACL��Ϣ
        aclKeyMap.put(acls, val);//���ACL��Ϣ
        return val;//�������
    }

    /**
     * converts a list of longs to a list of acls.
     *
     * @param longVal
     *            the list of longs
     * @return a list of ACLs that map to longs
     */
    public synchronized List<ACL> convertLong(Long longVal) {
        if (longVal == null)
            return null;
        if (longVal == -1L)
            return Ids.OPEN_ACL_UNSAFE;
        List<ACL> acls = longKeyMap.get(longVal);
        if (acls == null) {
            LOG.error("ERROR: ACL not available for long " + longVal);
            throw new RuntimeException("Failed to fetch acls for " + longVal);
        }
        return acls;
    }

    public Collection<Long> getSessions() {
        return ephemerals.keySet();
    }

    /**
     * just an accessor method to allow raw creation of datatree's from a bunch
     * of datanodes
     *
     * @param path
     *            the path of the datanode
     * @param node
     *            the datanode corresponding to this path
     */
    public void addDataNode(String path, DataNode node) {
        nodes.put(path, node);
    }

    public DataNode getNode(String path) {
        return nodes.get(path);
    }

    public int getNodeCount() {
        return nodes.size();
    }

    public int getWatchCount() {
        return dataWatches.size() + childWatches.size();
    }

    public int getEphemeralsCount() {
        Map<Long, HashSet<String>> map = this.getEphemeralsMap();
        int result = 0;
        for (HashSet<String> set : map.values()) {
            result += set.size();
        }
        return result;
    }

    /**
     * Get the size of the nodes based on path and data length.
     *
     * @return size of the data
     */
    public long approximateDataSize() {
        long result = 0;
        for (Map.Entry<String, DataNode> entry : nodes.entrySet()) {
            DataNode value = entry.getValue();
            synchronized (value) {
                result += entry.getKey().length();
                result += (value.data == null ? 0
                        : value.data.length);
            }
        }
        return result;
    }

    /**
     * This is a pointer to the root of the DataTree. It is the source of truth,
     * but we usually use the nodes hashmap to find nodes in the tree.
     */
    private DataNode root = new DataNode(null, new byte[0], -1L,
            new StatPersisted()); //���ڵ�

    /**
     * create a /zookeeper filesystem that is the proc filesystem of zookeeper
     */
    private DataNode procDataNode = new DataNode(root, new byte[0], -1L,
            new StatPersisted()); //zookeeper�ڵ�

    /**
     * create a /zookeeper/quota node for maintaining quota properties for
     * zookeeper
     */
    private DataNode quotaDataNode = new DataNode(procDataNode, new byte[0],
            -1L, new StatPersisted()); //quota�ڵ���Ϣ

    public DataTree() {
        /* Rather than fight it, let root have an alias */
        nodes.put("", root); //��ʼ�����ڵ�
        nodes.put(rootZookeeper, root); //��ʼ���ڵ�

        /** add the proc node and quota node */
        root.addChild(procChildZookeeper); //���ڵ���Ӻ��ӽڵ�/zookeeper
        nodes.put(procZookeeper, procDataNode);///zookeeper�ڵ���ӽ�map

        procDataNode.addChild(quotaChildZookeeper); ///zookeeper���quota�ڵ�
        nodes.put(quotaZookeeper, quotaDataNode); //quota�ڵ���ӽ�map
    }

    /**
     * is the path one of the special paths owned by zookeeper.
     *
     * @param path
     *            the path to be checked
     * @return true if a special path. false if not.
     */
    boolean isSpecialPath(String path) {
        if (rootZookeeper.equals(path) || procZookeeper.equals(path)
                || quotaZookeeper.equals(path)) {
            return true;
        }
        return false;
    }

    static public void copyStatPersisted(StatPersisted from, StatPersisted to) {
        to.setAversion(from.getAversion());
        to.setCtime(from.getCtime());
        to.setCversion(from.getCversion());
        to.setCzxid(from.getCzxid());
        to.setMtime(from.getMtime());
        to.setMzxid(from.getMzxid());
        to.setPzxid(from.getPzxid());
        to.setVersion(from.getVersion());
        to.setEphemeralOwner(from.getEphemeralOwner());
    }

    static public void copyStat(Stat from, Stat to) {
        to.setAversion(from.getAversion());
        to.setCtime(from.getCtime());
        to.setCversion(from.getCversion());
        to.setCzxid(from.getCzxid());
        to.setMtime(from.getMtime());
        to.setMzxid(from.getMzxid());
        to.setPzxid(from.getPzxid());
        to.setVersion(from.getVersion());
        to.setEphemeralOwner(from.getEphemeralOwner());
        to.setDataLength(from.getDataLength());
        to.setNumChildren(from.getNumChildren());
    }

    /**
     * update the count of this stat datanode
     *
     * @param lastPrefix
     *            the path of the node that is quotaed.
     * @param diff
     *            the diff to be added to the count
     */
    public void updateCount(String lastPrefix, int diff) {
        String statNode = Quotas.statPath(lastPrefix); //statpath
        DataNode node = nodes.get(statNode); //��ȡ�ڵ�
        StatsTrack updatedStat = null;
        if (node == null) {
            // should not happen
            LOG.error("Missing count node for stat " + statNode);
            return;
        }
        synchronized (node) {
            updatedStat = new StatsTrack(new String(node.data)); //���������Ϣ
            updatedStat.setCount(updatedStat.getCount() + diff); //����count
            node.data = updatedStat.toString().getBytes();//��ֵ���ڵ�
        }
        // now check if the counts match the quota
        String quotaNode = Quotas.quotaPath(lastPrefix);
        node = nodes.get(quotaNode);
        StatsTrack thisStats = null;
        if (node == null) {
            // should not happen
            LOG.error("Missing count node for quota " + quotaNode);
            return;
        }
        synchronized (node) {
            thisStats = new StatsTrack(new String(node.data));
        }
        if (thisStats.getCount() > -1 && (thisStats.getCount() < updatedStat.getCount())) {
            LOG
            .warn("Quota exceeded: " + lastPrefix + " count="
                    + updatedStat.getCount() + " limit="
                    + thisStats.getCount());
        }
    }

    /**
     * update the count of bytes of this stat datanode
     *
     * @param lastPrefix
     *            the path of the node that is quotaed
     * @param diff
     *            the diff to added to number of bytes
     * @throws IOException
     *             if path is not found
     */
    public void updateBytes(String lastPrefix, long diff) {
        String statNode = Quotas.statPath(lastPrefix);
        DataNode node = nodes.get(statNode);
        if (node == null) {
            // should never be null but just to make
            // findbugs happy
            LOG.error("Missing stat node for bytes " + statNode);
            return;
        }
        StatsTrack updatedStat = null;
        synchronized (node) {
            updatedStat = new StatsTrack(new String(node.data));
            updatedStat.setBytes(updatedStat.getBytes() + diff);
            node.data = updatedStat.toString().getBytes();
        }
        // now check if the bytes match the quota
        String quotaNode = Quotas.quotaPath(lastPrefix);
        node = nodes.get(quotaNode);
        if (node == null) {
            // should never be null but just to make
            // findbugs happy
            LOG.error("Missing quota node for bytes " + quotaNode);
            return;
        }
        StatsTrack thisStats = null;
        synchronized (node) {
            thisStats = new StatsTrack(new String(node.data));
        }
        if (thisStats.getBytes() > -1 && (thisStats.getBytes() < updatedStat.getBytes())) { //�����������
            LOG
            .warn("Quota exceeded: " + lastPrefix + " bytes="
                    + updatedStat.getBytes() + " limit="
                    + thisStats.getBytes());
        }
    }

    /**
     * @param path
     * @param data
     * @param acl
     * @param ephemeralOwner
     *            the session id that owns this node. -1 indicates this is not
     *            an ephemeral node.
     * @param zxid
     * @param time
     * @return the patch of the created node
     * @throws KeeperException
     */
    public String createNode(String path, byte data[], List<ACL> acl,
            long ephemeralOwner, int parentCVersion, long zxid, long time)
            throws KeeperException.NoNodeException,
            KeeperException.NodeExistsException {
        int lastSlash = path.lastIndexOf('/');
        String parentName = path.substring(0, lastSlash);//��ȡ���ڵ�����ƣ����ڵ��ȫ·����
        String childName = path.substring(lastSlash + 1);//������path�л�ȡ��Ҫ��ӵĽڵ���
        StatPersisted stat = new StatPersisted(); //����һ��ͳ����Ϣ
        stat.setCtime(time); //���ô���ʱ��
        stat.setMtime(time); //�����޸�ʱ��
        stat.setCzxid(zxid); //���ô�����zxid
        stat.setMzxid(zxid);//�����޸ĵ�zxid
        stat.setPzxid(zxid);//���øýڵ��ӽڵ����һ�α��޸ĵ�zxid
        stat.setVersion(0);//���ð汾��
        stat.setAversion(0);//����acl�汾��
        stat.setEphemeralOwner(ephemeralOwner); //������ʱ�ڵ��������
        DataNode parent = nodes.get(parentName); //��nodes�л�ȡ���ڵ�
        if (parent == null) {//������ڵ�Ϊ���׳��쳣
            throw new KeeperException.NoNodeException();
        }
        synchronized (parent) { //���ڵ㺢�ӽڵ�䶯�汾������
            Set<String> children = parent.getChildren(); //��ȡ���ڵ�����к��ӽڵ�
            if (children != null) {
                if (children.contains(childName)) { //�����ǰ�����Ѿ��Ǹ��ڵ���׳��ڵ�����쳣
                    throw new KeeperException.NodeExistsException();
                }
            }
            
            if (parentCVersion == -1) { //������븸�ڵ�İ汾��Ϊ-1
                parentCVersion = parent.stat.getCversion();//��ȡ���ڵ�İ汾��
                parentCVersion++;//��1
            }    
            parent.stat.setCversion(parentCVersion); //���ø��ڵ�汾��
            parent.stat.setPzxid(zxid); //������������zxid
            Long longval = convertAcls(acl);//������ӽڵ��Ȩ�޲�����long ��keyֵ
            DataNode child = new DataNode(parent, data, longval, stat); //�������ӽڵ�
            parent.addChild(childName); //��Ӻ��ӽڵ�
            nodes.put(path, child); //��ӽڵ�������path�ͽڵ�ӳ����
            if (ephemeralOwner != 0) { //����ڵ�������߲�Ϊ0˵���ýڵ�����ʱ�ڵ�
                HashSet<String> list = ephemerals.get(ephemeralOwner); //����ʱ�ڵ�ӻ�ȡ��ǰsessionId���еĽڵ�
                if (list == null) {
                    list = new HashSet<String>();
                    ephemerals.put(ephemeralOwner, list); //������ʱ�ڵ�List
                }
                synchronized (list) {
                    list.add(path); //����ǰ�ڵ���ӽ���ʱ�ڵ�list
                }
            }
        }
        // now check if its one of the zookeeper node child
        if (parentName.startsWith(quotaZookeeper)) { //������ڵ�������/zookeeper/quota��ʼ��˵����ӵ��������Ϣ
            // now check if its the limit node
            if (Quotas.limitNode.equals(childName)) { //���ӽڵ��Ƿ���zookeeper_limits�ڵ�
                // this is the limit node
                // get the parent and add it to the trie
                pTrie.addPath(parentName.substring(quotaZookeeper.length()));//���ڵ���Ϣ��ӽ��ֵ���
            }
            if (Quotas.statNode.equals(childName)) { //������ӽڵ���zookeeper_stats��˵�����״̬��Ϣ
                updateQuotaForPath(parentName
                        .substring(quotaZookeeper.length()));
            }
        }
        // also check to update the quotas for this node
        String lastPrefix; //
        if((lastPrefix = getMaxPrefixWithQuota(path)) != null) { //�ҵ����ӽڵ������һ��ӵ�����Ľڵ�
            // ok we have some match and need to update
            updateCount(lastPrefix, 1); //��������
            updateBytes(lastPrefix, data == null ? 0 : data.length); //�������ݳ���
        }
        dataWatches.triggerWatch(path, Event.EventType.NodeCreated); //�����ڵ㴴���¼�
        childWatches.triggerWatch(parentName.equals("") ? "/" : parentName,
                Event.EventType.NodeChildrenChanged); //�����ڵ�ı��¼�
        return path;
    }

    /**
     * remove the path from the datatree
     *
     * @param path
     *            the path to of the node to be deleted
     * @param zxid
     *            the current zxid
     * @throws KeeperException.NoNodeException
     */
    public void deleteNode(String path, long zxid)
            throws KeeperException.NoNodeException {
        int lastSlash = path.lastIndexOf('/');
        String parentName = path.substring(0, lastSlash); //��ȡ���ڵ�����
        String childName = path.substring(lastSlash + 1); //��ȡ�ڵ�����
        DataNode node = nodes.get(path); //��ȡ�ڵ�
        if (node == null) { //����ڵ㲻�����׳�NoNode�쳣
            throw new KeeperException.NoNodeException();
        }
        nodes.remove(path); //�ӽڵ�map�Ƴ��ýڵ�
        DataNode parent = nodes.get(parentName); //��ȡ���ڵ�
        if (parent == null) {//���ڵ㲻�����׳��쳣
            throw new KeeperException.NoNodeException();
        }
        synchronized (parent) {
            parent.removeChild(childName); //���ڵ㺢�ӽڵ��б��Ƴ��ýڵ�
            parent.stat.setPzxid(zxid);//�������������ӽڵ��zxid
            long eowner = node.stat.getEphemeralOwner();//��ȡ��ǰ���ӽڵ��������
            if (eowner != 0) {//�������ʱ�ڵ�
                HashSet<String> nodes = ephemerals.get(eowner);
                if (nodes != null) {
                    synchronized (nodes) {
                        nodes.remove(path); //���ýڵ����ʱ�ڵ��Ƴ�
                    }
                }
            }
            node.parent = null; //�ڵ�ĸ��ڵ�ֵΪ�� ����GC�������ã�
        }
        if (parentName.startsWith(procZookeeper)) { //��������ڵ�
            // delete the node in the trie.
            if (Quotas.limitNode.equals(childName)) {
                // we need to update the trie
                // as well
                pTrie.deletePath(parentName.substring(quotaZookeeper.length())); //ɾ�����ڵ�
            }
        }

        // also check to update the quotas for this node
        String lastPrefix;
        if((lastPrefix = getMaxPrefixWithQuota(path)) != null) { //������ҵ���������ڵ� �������ö�Ӧ��stats��Ϣ
            // ok we have some match and need to update
            updateCount(lastPrefix, -1);
            int bytes = 0;
            synchronized (node) {
                bytes = (node.data == null ? 0 : -(node.data.length));
            }
            updateBytes(lastPrefix, bytes);
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                    "dataWatches.triggerWatch " + path);
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                    "childWatches.triggerWatch " + parentName);
        }
        Set<Watcher> processed = dataWatches.triggerWatch(path,
                EventType.NodeDeleted); //��������
        childWatches.triggerWatch(path, EventType.NodeDeleted, processed); //�����ڵ�ɾ������
        childWatches.triggerWatch(parentName.equals("") ? "/" : parentName,
                EventType.NodeChildrenChanged); //�����ڵ�ı����
    }

    public Stat setData(String path, byte data[], int version, long zxid,
            long time) throws KeeperException.NoNodeException {
        Stat s = new Stat();
        DataNode n = nodes.get(path);//��ȡ�ڵ�
        if (n == null) { //����ڵ㲻�����׳��쳣
            throw new KeeperException.NoNodeException();
        }
        byte lastdata[] = null;
        synchronized (n) {
            lastdata = n.data; //��ȡԭʼ����
            n.data = data; //��������
            n.stat.setMtime(time);//�����޸�ʱ��
            n.stat.setMzxid(zxid);//�����޸�zxid
            n.stat.setVersion(version);//���ð汾
            n.copyStat(s);//copy�ڵ�ͳ����Ϣ
        }
        // now update if the path is in a quota subtree.
        String lastPrefix;
        if((lastPrefix = getMaxPrefixWithQuota(path)) != null) { //������������Ϣ���������Ϣ
          this.updateBytes(lastPrefix, (data == null ? 0 : data.length)
              - (lastdata == null ? 0 : lastdata.length));
        }
        dataWatches.triggerWatch(path, EventType.NodeDataChanged);//�������ݸı����
        return s;
    }

    /**
     * If there is a quota set, return the appropriate prefix for that quota
     * Else return null
     * @param path The ZK path to check for quota
     * @return Max quota prefix, or null if none
     */
    public String getMaxPrefixWithQuota(String path) {
        // do nothing for the root.
        // we are not keeping a quota on the zookeeper
        // root node for now.
        String lastPrefix = pTrie.findMaxPrefix(path);

        if (!rootZookeeper.equals(lastPrefix) && !("".equals(lastPrefix))) {
            return lastPrefix;
        }
        else {
            return null;
        }
    }

    public byte[] getData(String path, Stat stat, Watcher watcher)
            throws KeeperException.NoNodeException {
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.copyStat(stat);
            if (watcher != null) {
                dataWatches.addWatch(path, watcher);
            }
            return n.data;
        }
    }

    public Stat statNode(String path, Watcher watcher)
            throws KeeperException.NoNodeException {
        Stat stat = new Stat();
        DataNode n = nodes.get(path);
        if (watcher != null) {
            dataWatches.addWatch(path, watcher);
        }
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.copyStat(stat);
            return stat;
        }
    }

    public List<String> getChildren(String path, Stat stat, Watcher watcher)
            throws KeeperException.NoNodeException {
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            if (stat != null) {
                n.copyStat(stat);
            }
            ArrayList<String> children;
            Set<String> childs = n.getChildren();
            if (childs != null) {
                children = new ArrayList<String>(childs.size());
                children.addAll(childs);
            } else {
                children = new ArrayList<String>(0);
            }

            if (watcher != null) {
                childWatches.addWatch(path, watcher);
            }
            return children;
        }
    }

    public Stat setACL(String path, List<ACL> acl, int version)
            throws KeeperException.NoNodeException {
        Stat stat = new Stat();
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.stat.setAversion(version);
            n.acl = convertAcls(acl);
            n.copyStat(stat);
            return stat;
        }
    }

    @SuppressWarnings("unchecked")
    public List<ACL> getACL(String path, Stat stat)
            throws KeeperException.NoNodeException {
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.copyStat(stat);
            return new ArrayList<ACL>(convertLong(n.acl));
        }
    }

    static public class ProcessTxnResult {
        public long clientId; //�ͻ���ID

        public int cxid; //cxid

        public long zxid; //zxid

        public int err;//������

        public int type;//��������

        public String path;//����path

        public Stat stat;//ͳ����Ϣ

        public List<ProcessTxnResult> multiResult; //������������
        
        /**
         * Equality is defined as the clientId and the cxid being the same. This
         * allows us to use hash tables to track completion of transactions.
         *
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object o) {
            if (o instanceof ProcessTxnResult) {
                ProcessTxnResult other = (ProcessTxnResult) o;
                return other.clientId == clientId && other.cxid == cxid;
            }
            return false;
        }

        /**
         * See equals() to find the rational for how this hashcode is generated.
         *
         * @see ProcessTxnResult#equals(Object)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return (int) ((clientId ^ cxid) % Integer.MAX_VALUE);
        }

    }

    public volatile long lastProcessedZxid = 0; //�ڴ������һ��zxid

    public ProcessTxnResult processTxn(TxnHeader header, Record txn) //��������
    {
        ProcessTxnResult rc = new ProcessTxnResult(); //�����������񷵻ؽ��

        try {
            rc.clientId = header.getClientId(); //������ͷ�л�ȡ�ͻ���id������
            rc.cxid = header.getCxid(); //��ȡcxid������
            rc.zxid = header.getZxid();//��ȡzxid������
            rc.type = header.getType();//��ȡ�������Ͳ�����
            rc.err = 0; //���ô�����
            rc.multiResult = null;
            switch (header.getType()) { //�ж���������
                case OpCode.create: //�����ڵ�
                    CreateTxn createTxn = (CreateTxn) txn; //��txnתΪ��������
                    rc.path = createTxn.getPath();//��ȡpath
                    createNode(
                            createTxn.getPath(),
                            createTxn.getData(),
                            createTxn.getAcl(),
                            createTxn.getEphemeral() ? header.getClientId() : 0,
                            createTxn.getParentCVersion(),
                            header.getZxid(), header.getTime()); //��ӽڵ�
                    break;
                case OpCode.delete:
                    DeleteTxn deleteTxn = (DeleteTxn) txn;
                    rc.path = deleteTxn.getPath();
                    deleteNode(deleteTxn.getPath(), header.getZxid());
                    break;
                case OpCode.setData:
                    SetDataTxn setDataTxn = (SetDataTxn) txn;
                    rc.path = setDataTxn.getPath();
                    rc.stat = setData(setDataTxn.getPath(), setDataTxn
                            .getData(), setDataTxn.getVersion(), header
                            .getZxid(), header.getTime());
                    break;
                case OpCode.setACL:
                    SetACLTxn setACLTxn = (SetACLTxn) txn;
                    rc.path = setACLTxn.getPath();
                    rc.stat = setACL(setACLTxn.getPath(), setACLTxn.getAcl(),
                            setACLTxn.getVersion());
                    break;
                case OpCode.closeSession:
                    killSession(header.getClientId(), header.getZxid());
                    break;
                case OpCode.error:
                    ErrorTxn errTxn = (ErrorTxn) txn;
                    rc.err = errTxn.getErr();
                    break;
                case OpCode.check:
                    CheckVersionTxn checkTxn = (CheckVersionTxn) txn;
                    rc.path = checkTxn.getPath();
                    break;
                case OpCode.multi: //���һ����¼�а����������� ��Ҫ����¼�е������ֳ���һ��һ�����д���
                    MultiTxn multiTxn = (MultiTxn) txn ;
                    List<Txn> txns = multiTxn.getTxns();
                    rc.multiResult = new ArrayList<ProcessTxnResult>();
                    boolean failed = false;
                    for (Txn subtxn : txns) {
                        if (subtxn.getType() == OpCode.error) {
                            failed = true;
                            break;
                        }
                    }

                    boolean post_failed = false;
                    for (Txn subtxn : txns) {
                        ByteBuffer bb = ByteBuffer.wrap(subtxn.getData());
                        Record record = null;
                        switch (subtxn.getType()) {
                            case OpCode.create:
                                record = new CreateTxn();
                                break;
                            case OpCode.delete:
                                record = new DeleteTxn();
                                break;
                            case OpCode.setData:
                                record = new SetDataTxn();
                                break;
                            case OpCode.error:
                                record = new ErrorTxn();
                                post_failed = true;
                                break;
                            case OpCode.check:
                                record = new CheckVersionTxn();
                                break;
                            default:
                                throw new IOException("Invalid type of op: " + subtxn.getType());
                        }
                        assert(record != null);

                        ByteBufferInputStream.byteBuffer2Record(bb, record);
                       
                        if (failed && subtxn.getType() != OpCode.error){
                            int ec = post_failed ? Code.RUNTIMEINCONSISTENCY.intValue() 
                                                 : Code.OK.intValue();

                            subtxn.setType(OpCode.error);
                            record = new ErrorTxn(ec);
                        }

                        if (failed) {
                            assert(subtxn.getType() == OpCode.error) ;
                        }

                        TxnHeader subHdr = new TxnHeader(header.getClientId(), header.getCxid(),
                                                         header.getZxid(), header.getTime(), 
                                                         subtxn.getType());
                        ProcessTxnResult subRc = processTxn(subHdr, record); //һ��һ����������
                        rc.multiResult.add(subRc); //��Ӵ�����
                        if (subRc.err != 0 && rc.err == 0) {
                            rc.err = subRc.err ;
                        }
                    }
                    break;
            }
        } catch (KeeperException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed: " + header + ":" + txn, e);
            }
            rc.err = e.code().intValue();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed: " + header + ":" + txn, e);
            }
        }
        /*
         * A snapshot might be in progress while we are modifying the data
         * tree. If we set lastProcessedZxid prior to making corresponding
         * change to the tree, then the zxid associated with the snapshot
         * file will be ahead of its contents. Thus, while restoring from
         * the snapshot, the restore method will not apply the transaction
         * for zxid associated with the snapshot file, since the restore
         * method assumes that transaction to be present in the snapshot.
         *
         * To avoid this, we first apply the transaction and then modify
         * lastProcessedZxid.  During restore, we correctly handle the
         * case where the snapshot contains data ahead of the zxid associated
         * with the file.
         */
        if (rc.zxid > lastProcessedZxid) { //���zxid���ڵ�ǰ��zxid ��zxid����Ϊ��ǰ��zxid
        	lastProcessedZxid = rc.zxid;
        }

        /*
         * Snapshots are taken lazily. It can happen that the child
         * znodes of a parent are created after the parent
         * is serialized. Therefore, while replaying logs during restore, a
         * create might fail because the node was already
         * created.
         *
         * After seeing this failure, we should increment
         * the cversion of the parent znode since the parent was serialized
         * before its children.
         *
         * Note, such failures on DT should be seen only during
         * restore.
         */
        /**
         *  //�����л����ڵ�ʱ���������ӽڵ㱻���������ʱ�����л���ʱ����û�д������ӽڵ�ģ�
         *  �˴���һ�����
         *  �ڽ������л��ɿ��յ�ʱ����¼��ʼ��zxid �������л��������л����ڵ�Ȼ�������л��ӽڵ�
         *  �ڿ�ʼ���л���ʱ����������zxid�����ڿ�ʼ���п��յ�zxid���������ӽڵ㣬�ӽڵ�ͱ�¼���������ˣ����Ǵ˿̸��ڵ��cversion��״̬û�б仯
         */
        if (header.getType() == OpCode.create &&
                rc.err == Code.NODEEXISTS.intValue()) {
            LOG.debug("Adjusting parent cversion for Txn: " + header.getType() +
                    " path:" + rc.path + " err: " + rc.err);
            int lastSlash = rc.path.lastIndexOf('/');
            String parentName = rc.path.substring(0, lastSlash);
            CreateTxn cTxn = (CreateTxn)txn;
            try {
                setCversionPzxid(parentName, cTxn.getParentCVersion(),
                        header.getZxid()); //���Ӹ��ڵ�汾��
            } catch (KeeperException.NoNodeException e) {
                LOG.error("Failed to set parent cversion for: " +
                      parentName, e);
                rc.err = e.code().intValue();
            }
        } else if (rc.err != Code.OK.intValue()) {//�����쳣��ӡ��Ϣ
            LOG.debug("Ignoring processTxn failure hdr: " + header.getType() +
                  " : error: " + rc.err);
        }
        return rc; //���ش�����
    }

    void killSession(long session, long zxid) { //killSession
        // the list is already removed from the ephemerals
        // so we do not have to worry about synchronizing on
        // the list. This is only called from FinalRequestProcessor
        // so there is no need for synchronization. The list is not
        // changed here. Only create and delete change the list which
        // are again called from FinalRequestProcessor in sequence.
        HashSet<String> list = ephemerals.remove(session); //�Ƴ�������ص���ʱ�ڵ�
        if (list != null) {
            for (String path : list) {
                try {
                    deleteNode(path, zxid); //ɾ���ڵ�
                    if (LOG.isDebugEnabled()) {
                        LOG
                                .debug("Deleting ephemeral node " + path
                                        + " for session 0x"
                                        + Long.toHexString(session));
                    }
                } catch (NoNodeException e) {
                    LOG.warn("Ignoring NoNodeException for path " + path
                            + " while removing ephemeral for dead session 0x"
                            + Long.toHexString(session));
                }
            }
        }
    }

    /**
     * a encapsultaing class for return value
     */
    private static class Counts {
        long bytes;
        int count;
    }

    /**
     * this method gets the count of nodes and the bytes under a subtree
     *
     * @param path
     *            the path to be used
     * @param counts
     *            the int count
     */
    private void getCounts(String path, Counts counts) {
        DataNode node = getNode(path);
        if (node == null) {
            return;
        }
        String[] children = null;
        int len = 0;
        synchronized (node) {
            Set<String> childs = node.getChildren();
            if (childs != null) {
                children = childs.toArray(new String[childs.size()]);
            }
            len = (node.data == null ? 0 : node.data.length);
        }
        // add itself
        counts.count += 1;
        counts.bytes += len;
        if (children == null || children.length == 0) {
            return;
        }
        for (String child : children) {
            getCounts(path + "/" + child, counts);
        }
    }

    /**
     * update the quota for the given path
     *
     * @param path
     *            the path to be used
     */
    private void updateQuotaForPath(String path) { //�������ͳ����Ϣ
        Counts c = new Counts();  //��Ͻڵ�ͳ����Ϣ�� ��counts=xx,bytes=xx
        getCounts(path, c); //��ȡͳ����Ϣ����c��
        StatsTrack strack = new StatsTrack(); //���´���һ��ͳ��׷����
        strack.setBytes(c.bytes);//�������ݳ���
        strack.setCount(c.count);//�����ӽڵ�����
        String statPath = Quotas.quotaZookeeper + path + "/" + Quotas.statNode; //ƴ��ͳ�ƽڵ�·��
        DataNode node = getNode(statPath); //��ȡ�ڵ�
        // it should exist
        if (node == null) { //����ڵ�Ϊ�վ��沢����
            LOG.warn("Missing quota stat node " + statPath);
            return;
        }
        synchronized (node) {
            node.data = strack.toString().getBytes(); //���ýڵ�����Ϊ������
        }
    }

    /**
     * this method traverses the quota path and update the path trie and sets
     *
     * @param path
     */
    private void traverseNode(String path) {
        DataNode node = getNode(path); //��ȡpath
        String children[] = null;
        synchronized (node) {
            Set<String> childs = node.getChildren(); //��ȡ���е����ڵ�
            if (childs != null) {
                children = childs.toArray(new String[childs.size()]); //��ȡ���к�������
            }
        }
        if (children == null || children.length == 0) {
            // this node does not have a child
            // is the leaf node
            // check if its the leaf node
            String endString = "/" + Quotas.limitNode;
            if (path.endsWith(endString)) { //�����β��zookeeper_limits
                // ok this is the limit node
                // get the real node and update
                // the count and the bytes
                String realPath = path.substring(Quotas.quotaZookeeper
                        .length(), path.indexOf(endString));
                updateQuotaForPath(realPath); //���������Ϣ
                this.pTrie.addPath(realPath); //�ֵ�����ӽڵ�
            }
            return;
        }
        for (String child : children) { //�����ӽڵ�
            traverseNode(path + "/" + child);
        }
    }

    /**
     * this method sets up the path trie and sets up stats for quota nodes
     */
    private void setupQuota() {
        String quotaPath = Quotas.quotaZookeeper;
        DataNode node = getNode(quotaPath); //��ȡ���ڵ�
        if (node == null) { //���û�����ڵ�ֱ�ӷ���
            return;
        }
        traverseNode(quotaPath); //�������ڵ�
    }

    /**
     * this method uses a stringbuilder to create a new path for children. This
     * is faster than string appends ( str1 + str2).
     * ���л��ڵ��ʱ�������л����ڵ�Ȼ�����л����ӽڵ�
     *
     * @param oa
     *            OutputArchive to write to.
     * @param path
     *            a string builder.
     * @throws IOException
     * @throws InterruptedException
     */
    void serializeNode(OutputArchive oa, StringBuilder path) throws IOException {
        String pathString = path.toString(); //�ڵ�
        DataNode node = getNode(pathString);//��ȡ��ǰ�ڵ�
        if (node == null) {
            return;
        }
        String children[] = null;
        DataNode nodeCopy;
        synchronized (node) {
            scount++;
            StatPersisted statCopy = new StatPersisted();  //�־û�ͳ��
            copyStatPersisted(node.stat, statCopy); //���ýڵ�ͳ����Ϣ��statCopy��
            //we do not need to make a copy of node.data because the contents
            //are never changed
            nodeCopy = new DataNode(node.parent, node.data, node.acl, statCopy); //copy ��ǰ�ڵ���Ϣ
            Set<String> childs = node.getChildren(); //��ȡ���ӽڵ�����
            if (childs != null) {
                children = childs.toArray(new String[childs.size()]);
            }
        }
        oa.writeString(pathString, "path"); //д�븸�ڵ�
        oa.writeRecord(nodeCopy, "node"); //д��nodeCopy
        path.append('/');
        int off = path.length();
        if (children != null) { //������ӽڵ㲻Ϊ�տ�ʼ���л����ӽڵ�
            for (String child : children) {
                // since this is single buffer being resused
                // we need
                // to truncate the previous bytes of string.
                path.delete(off, Integer.MAX_VALUE); //ȥ����������
                path.append(child);
                serializeNode(oa, path); //���л����ӽڵ�
            }
        }
    }

    int scount; //�����л��ڵ�����

    public boolean initialized = false; //�Ƿ��ʼ��

    private void deserializeList(Map<Long, List<ACL>> longKeyMap,
            InputArchive ia) throws IOException {
        int i = ia.readInt("map");
        while (i > 0) {
            Long val = ia.readLong("long");
            if (aclIndex < val) {
                aclIndex = val;
            }
            List<ACL> aclList = new ArrayList<ACL>();
            Index j = ia.startVector("acls");
            while (!j.done()) {
                ACL acl = new ACL();
                acl.deserialize(ia, "acl");
                aclList.add(acl);
                j.incr();
            }
            longKeyMap.put(val, aclList);
            aclKeyMap.put(aclList, val);
            i--;
        }
    }

    private synchronized void serializeList(Map<Long, List<ACL>> longKeyMap,
            OutputArchive oa) throws IOException {
        oa.writeInt(longKeyMap.size(), "map"); //д��map��С
        Set<Map.Entry<Long, List<ACL>>> set = longKeyMap.entrySet();
        for (Map.Entry<Long, List<ACL>> val : set) {
            oa.writeLong(val.getKey(), "long"); //д�����
            List<ACL> aclList = val.getValue();
            oa.startVector(aclList, "acls");//��ʼд��ACL��Ϣ
            for (ACL acl : aclList) {
                acl.serialize(oa, "acl");  //д��
            }
            oa.endVector(aclList, "acls"); //д��ACL��Ϣ���
        }
    }

    public void serialize(OutputArchive oa, String tag) throws IOException {
        scount = 0;
        serializeList(longKeyMap, oa); //���л�ACL��Ϣ
        serializeNode(oa, new StringBuilder("")); //���л��ڵ�
        // / marks end of stream
        // we need to check if clear had been called in between the snapshot.
        if (root != null) {
            oa.writeString("/", "path");
        }
    }

    public void deserialize(InputArchive ia, String tag) throws IOException {
        deserializeList(longKeyMap, ia); //�����л�ACL
        nodes.clear(); //����ڵ���Ϣ
        pTrie.clear();//�����ֵ�����Ϣ
        String path = ia.readString("path"); //��ȡpath
        while (!path.equals("/")) {//ѭ�������л�ֱ��pathΪ"/"
            DataNode node = new DataNode(); //�����ڵ�
            ia.readRecord(node, "node");//�����л��ڵ�
            nodes.put(path, node);//���ڵ����nodes��
            int lastSlash = path.lastIndexOf('/'); //��ȡ·��path����"/"λ��
            if (lastSlash == -1) {//˵���Ǹ��ڵ�
                root = node;
            } else {
                String parentPath = path.substring(0, lastSlash); //��ȡ���ڵ�path
                node.parent = nodes.get(parentPath); //��ȡ���ڵ�node
                if (node.parent == null) { //������ڵ�Ϊ���׳��쳣
                    throw new IOException("Invalid Datatree, unable to find " +
                            "parent " + parentPath + " of path " + path);
                }
                node.parent.addChild(path.substring(lastSlash + 1)); //���ڵ���Ӻ��ӽڵ���Ϣ
                long eowner = node.stat.getEphemeralOwner(); //��ȡ�ڵ�������
                if (eowner != 0) { //�������ʱ�ڵ�
                    HashSet<String> list = ephemerals.get(eowner);
                    if (list == null) {
                        list = new HashSet<String>();
                        ephemerals.put(eowner, list); //����ʱ�ڵ������ʱ�ڵ�map
                    }
                    list.add(path);
                }
            }
            path = ia.readString("path"); //������ȡ
        }
        nodes.put("/", root); //�����Ӹ��ڵ�
        // we are done with deserializing the
        // the datatree
        // update the quotas - create path trie
        // and also update the stat nodes
        setupQuota(); //�������ڵ�
    }

    /**
     * Summary of the watches on the datatree.
     * @param pwriter the output to write to
     */
    public synchronized void dumpWatchesSummary(PrintWriter pwriter) {
        pwriter.print(dataWatches.toString());
    }

    /**
     * Write a text dump of all the watches on the datatree.
     * Warning, this is expensive, use sparingly!
     * @param pwriter the output to write to
     */
    public synchronized void dumpWatches(PrintWriter pwriter, boolean byPath) {
        dataWatches.dumpWatches(pwriter, byPath);
    }

    /**
     * Write a text dump of all the ephemerals in the datatree.
     * @param pwriter the output to write to
     */
    public void dumpEphemerals(PrintWriter pwriter) {
        Set<Long> keys = ephemerals.keySet();
        pwriter.println("Sessions with Ephemerals ("
                + keys.size() + "):");
        for (long k : keys) {
            pwriter.print("0x" + Long.toHexString(k));
            pwriter.println(":");
            HashSet<String> tmp = ephemerals.get(k);
            if (tmp != null) {
                synchronized (tmp) {
                    for (String path : tmp) {
                        pwriter.println("\t" + path);
                    }
                }
            }
        }
    }

    public void removeCnxn(Watcher watcher) {
        dataWatches.removeWatcher(watcher);
        childWatches.removeWatcher(watcher);
    }

    public void clear() {
        root = null;
        nodes.clear();
        ephemerals.clear();
    }

    public void setWatches(long relativeZxid, List<String> dataWatches,
            List<String> existWatches, List<String> childWatches,
            Watcher watcher) {
        for (String path : dataWatches) {
            DataNode node = getNode(path);
            if (node == null) {
                watcher.process(new WatchedEvent(EventType.NodeDeleted,
                            KeeperState.SyncConnected, path));
            } else if (node.stat.getMzxid() > relativeZxid) {
                watcher.process(new WatchedEvent(EventType.NodeDataChanged,
                            KeeperState.SyncConnected, path));
            } else {
                this.dataWatches.addWatch(path, watcher);
            }
        }
        for (String path : existWatches) {
            DataNode node = getNode(path);
            if (node != null) {
                watcher.process(new WatchedEvent(EventType.NodeCreated,
                            KeeperState.SyncConnected, path));
            } else {
                this.dataWatches.addWatch(path, watcher);
            }
        }
        for (String path : childWatches) {
            DataNode node = getNode(path);
            if (node == null) {
                watcher.process(new WatchedEvent(EventType.NodeDeleted,
                            KeeperState.SyncConnected, path));
            } else if (node.stat.getPzxid() > relativeZxid) {
                watcher.process(new WatchedEvent(EventType.NodeChildrenChanged,
                            KeeperState.SyncConnected, path));
            } else {
                this.childWatches.addWatch(path, watcher);
            }
        }
    }

     /**
      * This method sets the Cversion and Pzxid for the specified node to the
      * values passed as arguments. The values are modified only if newCversion
      * is greater than the current Cversion. A NoNodeException is thrown if
      * a znode for the specified path is not found.
      *
      * @param path
      *     Full path to the znode whose Cversion needs to be modified.
      *     A "/" at the end of the path is ignored.
      * @param newCversion
      *     Value to be assigned to Cversion
      * @param zxid
      *     Value to be assigned to Pzxid
      * @throws KeeperException.NoNodeException
      *     If znode not found.
      **/
    public void setCversionPzxid(String path, int newCversion, long zxid)
        throws KeeperException.NoNodeException {
        if (path.endsWith("/")) {
           path = path.substring(0, path.length() - 1);
        }
        DataNode node = nodes.get(path);
        if (node == null) {
            throw new KeeperException.NoNodeException(path);
        }
        synchronized (node) {
            if(newCversion == -1) {
                newCversion = node.stat.getCversion() + 1;
            }
            if (newCversion > node.stat.getCversion()) {
                node.stat.setCversion(newCversion);
                node.stat.setPzxid(zxid);
            }
        }
    }
}
