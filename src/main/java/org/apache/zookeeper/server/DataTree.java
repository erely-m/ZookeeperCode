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
        new ConcurrentHashMap<String, DataNode>();   //所有的节点信息path和DataNode的映射

    private final WatchManager dataWatches = new WatchManager(); //数据观察者管理

    private final WatchManager childWatches = new WatchManager();//孩子节点观察者管理

    /** the root of zookeeper tree */
    private static final String rootZookeeper = "/"; //根节点

    /** the zookeeper nodes that acts as the management and status node **/
    private static final String procZookeeper = Quotas.procZookeeper; //zookeeper节点

    /** this will be the string thats stored as a child of root */
    private static final String procChildZookeeper = procZookeeper.substring(1); //去除"/"的zookeeper节点字符串

    /**
     * the zookeeper quota node that acts as the quota management node for
     * zookeeper
     */
    private static final String quotaZookeeper = Quotas.quotaZookeeper; //配额目录

    /** this will be the string thats stored as a child of /zookeeper */
    private static final String quotaChildZookeeper = quotaZookeeper
            .substring(procZookeeper.length() + 1); //去除"/zookeeper/"的quota节点字符串

    /**
     * the path trie that keeps track fo the quota nodes in this datatree
     */
    private final PathTrie pTrie = new PathTrie(); //字典树

    /**
     * This hashtable lists the paths of the ephemeral nodes of a session.
     */
    private final Map<Long, HashSet<String>> ephemerals =
        new ConcurrentHashMap<Long, HashSet<String>>(); //所有的临时节点

    /**
     * this is map from longs to acl's. It saves acl's being stored for each
     * datanode.
     */
    public final Map<Long, List<ACL>> longKeyMap =
        new HashMap<Long, List<ACL>>(); //ACL信息key之为一个long型序号，long型值存储在DataNode中

    /**
     * this a map from acls to long.
     */
    public final Map<List<ACL>, Long> aclKeyMap =
        new HashMap<List<ACL>, Long>(); //ACL信息 和上面的正好相反key值为ACL信息

    /**
     * these are the number of acls that we have in the datatree
     */
    protected long aclIndex = 0; //ACL总数

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
    public synchronized Long convertAcls(List<ACL> acls) { //添加ACL信息
        if (acls == null) //如果ACL为空返回-1
            return -1L;
        // get the value from the map
        Long ret = aclKeyMap.get(acls); //获取ACL，如果有一样的ACL信息就复用，没有必要在重新创建一个
        // could not find the map
        if (ret != null)//如果序号不为空直接返回
            return ret;
        long val = incrementIndex(); //序号加1
        longKeyMap.put(val, acls);//添加ACL信息
        aclKeyMap.put(acls, val);//添加ACL信息
        return val;//返回序号
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
            new StatPersisted()); //根节点

    /**
     * create a /zookeeper filesystem that is the proc filesystem of zookeeper
     */
    private DataNode procDataNode = new DataNode(root, new byte[0], -1L,
            new StatPersisted()); //zookeeper节点

    /**
     * create a /zookeeper/quota node for maintaining quota properties for
     * zookeeper
     */
    private DataNode quotaDataNode = new DataNode(procDataNode, new byte[0],
            -1L, new StatPersisted()); //quota节点信息

    public DataTree() {
        /* Rather than fight it, let root have an alias */
        nodes.put("", root); //初始化根节点
        nodes.put(rootZookeeper, root); //初始化节点

        /** add the proc node and quota node */
        root.addChild(procChildZookeeper); //根节点添加孩子节点/zookeeper
        nodes.put(procZookeeper, procDataNode);///zookeeper节点添加进map

        procDataNode.addChild(quotaChildZookeeper); ///zookeeper添加quota节点
        nodes.put(quotaZookeeper, quotaDataNode); //quota节点添加进map
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
        DataNode node = nodes.get(statNode); //获取节点
        StatsTrack updatedStat = null;
        if (node == null) {
            // should not happen
            LOG.error("Missing count node for stat " + statNode);
            return;
        }
        synchronized (node) {
            updatedStat = new StatsTrack(new String(node.data)); //更新配额信息
            updatedStat.setCount(updatedStat.getCount() + diff); //更新count
            node.data = updatedStat.toString().getBytes();//赋值给节点
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
        if (thisStats.getBytes() > -1 && (thisStats.getBytes() < updatedStat.getBytes())) { //如果大于配额警告
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
        String parentName = path.substring(0, lastSlash);//获取父节点的名称（父节点的全路径）
        String childName = path.substring(lastSlash + 1);//从整个path中获取需要添加的节点名
        StatPersisted stat = new StatPersisted(); //创建一个统计信息
        stat.setCtime(time); //设置创建时间
        stat.setMtime(time); //设置修改时间
        stat.setCzxid(zxid); //设置创建的zxid
        stat.setMzxid(zxid);//设置修改的zxid
        stat.setPzxid(zxid);//设置该节点子节点最后一次被修改的zxid
        stat.setVersion(0);//设置版本号
        stat.setAversion(0);//设置acl版本号
        stat.setEphemeralOwner(ephemeralOwner); //设置临时节点的所有者
        DataNode parent = nodes.get(parentName); //从nodes中获取父节点
        if (parent == null) {//如果父节点为空抛出异常
            throw new KeeperException.NoNodeException();
        }
        synchronized (parent) { //父节点孩子节点变动版本号增加
            Set<String> children = parent.getChildren(); //获取父节点的所有孩子节点
            if (children != null) {
                if (children.contains(childName)) { //如果当前孩子已经是父节点的抛出节点存在异常
                    throw new KeeperException.NodeExistsException();
                }
            }
            
            if (parentCVersion == -1) { //如果传入父节点的版本号为-1
                parentCVersion = parent.stat.getCversion();//获取父节点的版本号
                parentCVersion++;//加1
            }    
            parent.stat.setCversion(parentCVersion); //设置父节点版本号
            parent.stat.setPzxid(zxid); //设置最后操作的zxid
            Long longval = convertAcls(acl);//设置添加节点的权限并返回long 的key值
            DataNode child = new DataNode(parent, data, longval, stat); //创建孩子节点
            parent.addChild(childName); //添加孩子节点
            nodes.put(path, child); //添加节点至所有path和节点映射中
            if (ephemeralOwner != 0) { //如果节点的所有者不为0说明该节点是临时节点
                HashSet<String> list = ephemerals.get(ephemeralOwner); //从临时节点从获取当前sessionId所有的节点
                if (list == null) {
                    list = new HashSet<String>();
                    ephemerals.put(ephemeralOwner, list); //创建临时节点List
                }
                synchronized (list) {
                    list.add(path); //将当前节点添加进临时节点list
                }
            }
        }
        // now check if its one of the zookeeper node child
        if (parentName.startsWith(quotaZookeeper)) { //如果父节点名是由/zookeeper/quota开始的说明添加的是配额信息
            // now check if its the limit node
            if (Quotas.limitNode.equals(childName)) { //孩子节点是否是zookeeper_limits节点
                // this is the limit node
                // get the parent and add it to the trie
                pTrie.addPath(parentName.substring(quotaZookeeper.length()));//将节点信息添加进字典树
            }
            if (Quotas.statNode.equals(childName)) { //如果孩子节点是zookeeper_stats则说明添加状态信息
                updateQuotaForPath(parentName
                        .substring(quotaZookeeper.length()));
            }
        }
        // also check to update the quotas for this node
        String lastPrefix; //
        if((lastPrefix = getMaxPrefixWithQuota(path)) != null) { //找到孩子节点最近的一个拥有配额的节点
            // ok we have some match and need to update
            updateCount(lastPrefix, 1); //增加数量
            updateBytes(lastPrefix, data == null ? 0 : data.length); //增加数据长度
        }
        dataWatches.triggerWatch(path, Event.EventType.NodeCreated); //触发节点创建事件
        childWatches.triggerWatch(parentName.equals("") ? "/" : parentName,
                Event.EventType.NodeChildrenChanged); //触发节点改变事件
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
        String parentName = path.substring(0, lastSlash); //获取父节点名称
        String childName = path.substring(lastSlash + 1); //获取节点名称
        DataNode node = nodes.get(path); //获取节点
        if (node == null) { //如果节点不存在抛出NoNode异常
            throw new KeeperException.NoNodeException();
        }
        nodes.remove(path); //从节点map移除该节点
        DataNode parent = nodes.get(parentName); //获取父节点
        if (parent == null) {//父节点不存在抛出异常
            throw new KeeperException.NoNodeException();
        }
        synchronized (parent) {
            parent.removeChild(childName); //父节点孩子节点列表移除该节点
            parent.stat.setPzxid(zxid);//设置最后操作孩子节点的zxid
            long eowner = node.stat.getEphemeralOwner();//获取当前孩子节点的所属者
            if (eowner != 0) {//如果是临时节点
                HashSet<String> nodes = ephemerals.get(eowner);
                if (nodes != null) {
                    synchronized (nodes) {
                        nodes.remove(path); //将该节点从临时节点移除
                    }
                }
            }
            node.parent = null; //节点的父节点值为空 便于GC（无引用）
        }
        if (parentName.startsWith(procZookeeper)) { //如果是配额节点
            // delete the node in the trie.
            if (Quotas.limitNode.equals(childName)) {
                // we need to update the trie
                // as well
                pTrie.deletePath(parentName.substring(quotaZookeeper.length())); //删除配额节点
            }
        }

        // also check to update the quotas for this node
        String lastPrefix;
        if((lastPrefix = getMaxPrefixWithQuota(path)) != null) { //如果能找到最近的配额节点 重新设置对应的stats信息
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
                EventType.NodeDeleted); //触发监听
        childWatches.triggerWatch(path, EventType.NodeDeleted, processed); //触发节点删除监听
        childWatches.triggerWatch(parentName.equals("") ? "/" : parentName,
                EventType.NodeChildrenChanged); //触发节点改变监听
    }

    public Stat setData(String path, byte data[], int version, long zxid,
            long time) throws KeeperException.NoNodeException {
        Stat s = new Stat();
        DataNode n = nodes.get(path);//获取节点
        if (n == null) { //如果节点不存在抛出异常
            throw new KeeperException.NoNodeException();
        }
        byte lastdata[] = null;
        synchronized (n) {
            lastdata = n.data; //获取原始数据
            n.data = data; //更新数据
            n.stat.setMtime(time);//设置修改时间
            n.stat.setMzxid(zxid);//设置修改zxid
            n.stat.setVersion(version);//设置版本
            n.copyStat(s);//copy节点统计信息
        }
        // now update if the path is in a quota subtree.
        String lastPrefix;
        if((lastPrefix = getMaxPrefixWithQuota(path)) != null) { //如果存在配额信息更新配额信息
          this.updateBytes(lastPrefix, (data == null ? 0 : data.length)
              - (lastdata == null ? 0 : lastdata.length));
        }
        dataWatches.triggerWatch(path, EventType.NodeDataChanged);//触发数据改变监听
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
        public long clientId; //客户端ID

        public int cxid; //cxid

        public long zxid; //zxid

        public int err;//错误码

        public int type;//事务类型

        public String path;//操作path

        public Stat stat;//统计信息

        public List<ProcessTxnResult> multiResult; //多条事务处理结果
        
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

    public volatile long lastProcessedZxid = 0; //内存中最后一个zxid

    public ProcessTxnResult processTxn(TxnHeader header, Record txn) //处理事务
    {
        ProcessTxnResult rc = new ProcessTxnResult(); //创建处理事务返回结果

        try {
            rc.clientId = header.getClientId(); //从事务头中获取客户端id并设置
            rc.cxid = header.getCxid(); //获取cxid并设置
            rc.zxid = header.getZxid();//获取zxid并设置
            rc.type = header.getType();//获取操作类型并设置
            rc.err = 0; //设置错误码
            rc.multiResult = null;
            switch (header.getType()) { //判断事务类型
                case OpCode.create: //创建节点
                    CreateTxn createTxn = (CreateTxn) txn; //将txn转为创建事务
                    rc.path = createTxn.getPath();//获取path
                    createNode(
                            createTxn.getPath(),
                            createTxn.getData(),
                            createTxn.getAcl(),
                            createTxn.getEphemeral() ? header.getClientId() : 0,
                            createTxn.getParentCVersion(),
                            header.getZxid(), header.getTime()); //添加节点
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
                case OpCode.multi: //如果一条记录中包含多条事务 需要将记录中的事务拆分出来一条一条进行处理
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
                        ProcessTxnResult subRc = processTxn(subHdr, record); //一条一条处理事务
                        rc.multiResult.add(subRc); //添加处理结果
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
        if (rc.zxid > lastProcessedZxid) { //如果zxid大于当前的zxid 将zxid设置为当前的zxid
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
         *  //在序列化父节点时，可能有子节点被创建，这个时候序列化的时候是没有创建该子节点的？
         *  此处有一种情况
         *  在进行序列化成快照的时候会记录开始的zxid 由于序列化是先序列化父节点然后在序列化子节点
         *  在开始序列化的时候有其他的zxid（大于开始进行快照的zxid）创建了子节点，子节点就被录到快照中了，但是此刻父节点的cversion的状态没有变化
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
                        header.getZxid()); //增加父节点版本号
            } catch (KeeperException.NoNodeException e) {
                LOG.error("Failed to set parent cversion for: " +
                      parentName, e);
                rc.err = e.code().intValue();
            }
        } else if (rc.err != Code.OK.intValue()) {//处理异常打印信息
            LOG.debug("Ignoring processTxn failure hdr: " + header.getType() +
                  " : error: " + rc.err);
        }
        return rc; //返回处理结果
    }

    void killSession(long session, long zxid) { //killSession
        // the list is already removed from the ephemerals
        // so we do not have to worry about synchronizing on
        // the list. This is only called from FinalRequestProcessor
        // so there is no need for synchronization. The list is not
        // changed here. Only create and delete change the list which
        // are again called from FinalRequestProcessor in sequence.
        HashSet<String> list = ephemerals.remove(session); //移除所有相关的临时节点
        if (list != null) {
            for (String path : list) {
                try {
                    deleteNode(path, zxid); //删除节点
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
    private void updateQuotaForPath(String path) { //更新配额统计信息
        Counts c = new Counts();  //配合节点统计信息类 即counts=xx,bytes=xx
        getCounts(path, c); //获取统计信息放入c中
        StatsTrack strack = new StatsTrack(); //重新创建一个统计追踪器
        strack.setBytes(c.bytes);//设置数据长度
        strack.setCount(c.count);//设置子节点数量
        String statPath = Quotas.quotaZookeeper + path + "/" + Quotas.statNode; //拼接统计节点路径
        DataNode node = getNode(statPath); //获取节点
        // it should exist
        if (node == null) { //如果节点为空警告并返回
            LOG.warn("Missing quota stat node " + statPath);
            return;
        }
        synchronized (node) {
            node.data = strack.toString().getBytes(); //设置节点数据为新数据
        }
    }

    /**
     * this method traverses the quota path and update the path trie and sets
     *
     * @param path
     */
    private void traverseNode(String path) {
        DataNode node = getNode(path); //获取path
        String children[] = null;
        synchronized (node) {
            Set<String> childs = node.getChildren(); //获取所有的配额节点
            if (childs != null) {
                children = childs.toArray(new String[childs.size()]); //获取所有孩子名称
            }
        }
        if (children == null || children.length == 0) {
            // this node does not have a child
            // is the leaf node
            // check if its the leaf node
            String endString = "/" + Quotas.limitNode;
            if (path.endsWith(endString)) { //如果结尾是zookeeper_limits
                // ok this is the limit node
                // get the real node and update
                // the count and the bytes
                String realPath = path.substring(Quotas.quotaZookeeper
                        .length(), path.indexOf(endString));
                updateQuotaForPath(realPath); //更新配额信息
                this.pTrie.addPath(realPath); //字典树添加节点
            }
            return;
        }
        for (String child : children) { //处理子节点
            traverseNode(path + "/" + child);
        }
    }

    /**
     * this method sets up the path trie and sets up stats for quota nodes
     */
    private void setupQuota() {
        String quotaPath = Quotas.quotaZookeeper;
        DataNode node = getNode(quotaPath); //获取配额节点
        if (node == null) { //如果没有配额节点直接返回
            return;
        }
        traverseNode(quotaPath); //处理配额节点
    }

    /**
     * this method uses a stringbuilder to create a new path for children. This
     * is faster than string appends ( str1 + str2).
     * 序列化节点的时候先序列化父节点然后序列化孩子节点
     *
     * @param oa
     *            OutputArchive to write to.
     * @param path
     *            a string builder.
     * @throws IOException
     * @throws InterruptedException
     */
    void serializeNode(OutputArchive oa, StringBuilder path) throws IOException {
        String pathString = path.toString(); //节点
        DataNode node = getNode(pathString);//获取当前节点
        if (node == null) {
            return;
        }
        String children[] = null;
        DataNode nodeCopy;
        synchronized (node) {
            scount++;
            StatPersisted statCopy = new StatPersisted();  //持久化统计
            copyStatPersisted(node.stat, statCopy); //设置节点统计信息至statCopy中
            //we do not need to make a copy of node.data because the contents
            //are never changed
            nodeCopy = new DataNode(node.parent, node.data, node.acl, statCopy); //copy 当前节点信息
            Set<String> childs = node.getChildren(); //获取孩子节点名称
            if (childs != null) {
                children = childs.toArray(new String[childs.size()]);
            }
        }
        oa.writeString(pathString, "path"); //写入父节点
        oa.writeRecord(nodeCopy, "node"); //写入nodeCopy
        path.append('/');
        int off = path.length();
        if (children != null) { //如果孩子节点不为空开始序列化孩子节点
            for (String child : children) {
                // since this is single buffer being resused
                // we need
                // to truncate the previous bytes of string.
                path.delete(off, Integer.MAX_VALUE); //去除无用数据
                path.append(child);
                serializeNode(oa, path); //序列化孩子节点
            }
        }
    }

    int scount; //反序列化节点数量

    public boolean initialized = false; //是否初始化

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
        oa.writeInt(longKeyMap.size(), "map"); //写入map大小
        Set<Map.Entry<Long, List<ACL>>> set = longKeyMap.entrySet();
        for (Map.Entry<Long, List<ACL>> val : set) {
            oa.writeLong(val.getKey(), "long"); //写入序号
            List<ACL> aclList = val.getValue();
            oa.startVector(aclList, "acls");//开始写入ACL信息
            for (ACL acl : aclList) {
                acl.serialize(oa, "acl");  //写入
            }
            oa.endVector(aclList, "acls"); //写入ACL信息完成
        }
    }

    public void serialize(OutputArchive oa, String tag) throws IOException {
        scount = 0;
        serializeList(longKeyMap, oa); //序列化ACL信息
        serializeNode(oa, new StringBuilder("")); //序列化节点
        // / marks end of stream
        // we need to check if clear had been called in between the snapshot.
        if (root != null) {
            oa.writeString("/", "path");
        }
    }

    public void deserialize(InputArchive ia, String tag) throws IOException {
        deserializeList(longKeyMap, ia); //反序列化ACL
        nodes.clear(); //清理节点信息
        pTrie.clear();//清理字典树信息
        String path = ia.readString("path"); //读取path
        while (!path.equals("/")) {//循环反序列化直到path为"/"
            DataNode node = new DataNode(); //创建节点
            ia.readRecord(node, "node");//反序列化节点
            nodes.put(path, node);//将节点加入nodes中
            int lastSlash = path.lastIndexOf('/'); //获取路径path最后的"/"位置
            if (lastSlash == -1) {//说明是根节点
                root = node;
            } else {
                String parentPath = path.substring(0, lastSlash); //获取父节点path
                node.parent = nodes.get(parentPath); //获取父节点node
                if (node.parent == null) { //如果父节点为空抛出异常
                    throw new IOException("Invalid Datatree, unable to find " +
                            "parent " + parentPath + " of path " + path);
                }
                node.parent.addChild(path.substring(lastSlash + 1)); //父节点添加孩子节点信息
                long eowner = node.stat.getEphemeralOwner(); //获取节点所属者
                if (eowner != 0) { //如果是临时节点
                    HashSet<String> list = ephemerals.get(eowner);
                    if (list == null) {
                        list = new HashSet<String>();
                        ephemerals.put(eowner, list); //将临时节点加入临时节点map
                    }
                    list.add(path);
                }
            }
            path = ia.readString("path"); //继续读取
        }
        nodes.put("/", root); //最后添加根节点
        // we are done with deserializing the
        // the datatree
        // update the quotas - create path trie
        // and also update the stat nodes
        setupQuota(); //处理配额节点
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
