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


/**
 * Basic Server Statistics
 */
public class ServerStats {
    private long packetsSent; //发送包的个数
    private long packetsReceived; //接受包的个数
    private long maxLatency; //最长延迟
    private long minLatency = Long.MAX_VALUE; //最短延迟
    private long totalLatency = 0; //总延迟
    private long count = 0; //请求次数

    private final Provider provider; //Provider对象，提供部分统计数据

    public interface Provider {
        public long getOutstandingRequests();
        public long getLastProcessedZxid();
        public String getState();
        public int getNumAliveConnections();
    }
    
    public ServerStats(Provider provider) {
        this.provider = provider;
    }
    
    // getters
    synchronized public long getMinLatency() {
        return minLatency == Long.MAX_VALUE ? 0 : minLatency;
    }

    synchronized public long getAvgLatency() {
        if (count != 0) {
            return totalLatency / count;
        }
        return 0;
    }

    synchronized public long getMaxLatency() {
        return maxLatency;
    }

    public long getOutstandingRequests() {
        return provider.getOutstandingRequests();
    }
    
    public long getLastProcessedZxid(){
        return provider.getLastProcessedZxid();
    }
    
    synchronized public long getPacketsReceived() {
        return packetsReceived;
    }

    synchronized public long getPacketsSent() {
        return packetsSent;
    }

    public String getServerState() {
        return provider.getState();
    }
    
    /** The number of client connections alive to this server */
    public int getNumAliveClientConnections() {
    	return provider.getNumAliveConnections();
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Latency min/avg/max: " + getMinLatency() + "/"
                + getAvgLatency() + "/" + getMaxLatency() + "\n");
        sb.append("Received: " + getPacketsReceived() + "\n");
        sb.append("Sent: " + getPacketsSent() + "\n");
        sb.append("Connections: " + getNumAliveClientConnections() + "\n");

        if (provider != null) {
            sb.append("Outstanding: " + getOutstandingRequests() + "\n");
            sb.append("Zxid: 0x"+ Long.toHexString(getLastProcessedZxid())+ "\n");
        }
        sb.append("Mode: " + getServerState() + "\n");
        return sb.toString();
    }
    // mutators
    synchronized void updateLatency(long requestCreateTime) { //更新延迟信息
        long latency = System.currentTimeMillis() - requestCreateTime;
        totalLatency += latency;
        count++;
        if (latency < minLatency) {
            minLatency = latency;
        }
        if (latency > maxLatency) {
            maxLatency = latency;
        }
    }
    synchronized public void resetLatency(){ //重置延迟信息
        totalLatency = 0;
        count = 0;
        maxLatency = 0;
        minLatency = Long.MAX_VALUE;
    }
    synchronized public void resetMaxLatency(){
        maxLatency = getMinLatency();
    }
    synchronized public void incrementPacketsReceived() {
        packetsReceived++;
    }
    synchronized public void incrementPacketsSent() {
        packetsSent++;
    }
    synchronized public void resetRequestCounters(){
        packetsReceived = 0;
        packetsSent = 0;
    }
    synchronized public void reset() {
        resetLatency();
        resetRequestCounters();
    }

}
