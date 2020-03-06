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

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * This class manages watches. It allows watches to be associated with a string
 * and removes watchers and their watches in addition to managing triggers.
 */
/**
 * 提供两个维度的映射管理，节点path对应的所有观察者，观察者对应的所有节点path维度。
 */

public class WatchManager {
    private static final Logger LOG = LoggerFactory.getLogger(WatchManager.class);

    private final HashMap<String, HashSet<Watcher>> watchTable =
        new HashMap<String, HashSet<Watcher>>(); //观察表

    private final HashMap<Watcher, HashSet<String>> watch2Paths =
        new HashMap<Watcher, HashSet<String>>();//观察者和节点间的映射

        public synchronized int size(){//计算观察者个数
            int result = 0;
            for(Set<Watcher> watches : watchTable.values()) {
                result += watches.size();
            }
            return result;
        }
    //添加观察者
    public synchronized void addWatch(String path, Watcher watcher) {
        HashSet<Watcher> list = watchTable.get(path); //通过节点路径path获取观察者列表
        if (list == null) { //如果为空
            // don't waste memory if there are few watches on a node
            // rehash when the 4th entry is added, doubling size thereafter
            // seems like a good compromise
            list = new HashSet<Watcher>(4);
            watchTable.put(path, list); //创建一个新的映射关系List大小为4
        }
        list.add(watcher); //将观察者加入里表中

        HashSet<String> paths = watch2Paths.get(watcher); //同理通过观察者获取所有的节点列表
        if (paths == null) { //如果为空新建一个映射关系
            // cnxns typically have many watches, so use default cap here
            paths = new HashSet<String>();
            watch2Paths.put(watcher, paths);
        }
        paths.add(path); //将path添加进该观察者的映射关系中
    }
    //移除观察者
    public synchronized void removeWatcher(Watcher watcher) {
        HashSet<String> paths = watch2Paths.remove(watcher); //移除观察者并获取到相关的path
        if (paths == null) {//如果path为空直接返回
            return;
        }
        for (String p : paths) { //将所有节点path关联的观察者移除移除完成后将节点path和观察者们的映射移除
            HashSet<Watcher> list = watchTable.get(p);
            if (list != null) {
                list.remove(watcher);
                if (list.size() == 0) {
                    watchTable.remove(p);
                }
            }
        }
    }
    //触发事件
    public Set<Watcher> triggerWatch(String path, EventType type) {
        return triggerWatch(path, type, null);
    }

    public Set<Watcher> triggerWatch(String path, EventType type, Set<Watcher> supress) {
        WatchedEvent e = new WatchedEvent(type,
                KeeperState.SyncConnected, path); //创建一个观察者事件 zk状态为连接
        HashSet<Watcher> watchers; 
        synchronized (this) {
            watchers = watchTable.remove(path); //通过path获取到所有的观察者 caution：此处是通过移除path获取对应watcher 说明watcher的一次性
            if (watchers == null || watchers.isEmpty()) { //如果没有观察者或者观察者为空直接返回
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logTraceMessage(LOG,
                            ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                            "No watchers for " + path);
                }
                return null;
            }
            for (Watcher w : watchers) {//如果观察者有 这一次移除观察者中对应的path
                HashSet<String> paths = watch2Paths.get(w);
                if (paths != null) {
                    paths.remove(path);
                }
            }
        }
        //处理时间
        for (Watcher w : watchers) {
            if (supress != null && supress.contains(w)) { //如果禁止列表包含该观察者则不触发事件
                continue;
            }
            w.process(e); //观察者处理事件
        }
        return watchers; //返回所有的观察者们
    }

    /**
     * Brief description of this object.
     */
    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(watch2Paths.size()).append(" connections watching ")
            .append(watchTable.size()).append(" paths\n");

        int total = 0;
        for (HashSet<String> paths : watch2Paths.values()) {
            total += paths.size();
        }
        sb.append("Total watches:").append(total);

        return sb.toString();
    }

    /**
     * String representation of watches. Warning, may be large!
     * @param byPath iff true output watches by paths, otw output
     * watches by connection
     * @return string representation of watches
     */
    public synchronized void dumpWatches(PrintWriter pwriter, boolean byPath) {
        if (byPath) {
            for (Entry<String, HashSet<Watcher>> e : watchTable.entrySet()) {
                pwriter.println(e.getKey());
                for (Watcher w : e.getValue()) {
                    pwriter.print("\t0x");
                    pwriter.print(Long.toHexString(((ServerCnxn)w).getSessionId()));
                    pwriter.print("\n");
                }
            }
        } else {
            for (Entry<Watcher, HashSet<String>> e : watch2Paths.entrySet()) {
                pwriter.print("0x");
                pwriter.println(Long.toHexString(((ServerCnxn)e.getKey()).getSessionId()));
                for (String path : e.getValue()) {
                    pwriter.print("\t");
                    pwriter.println(path);
                }
            }
        }
    }
}
