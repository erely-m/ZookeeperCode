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

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    private final ZooKeeperServer zks; //zk������
    private final LinkedBlockingQueue<Request> queuedRequests =
        new LinkedBlockingQueue<Request>(); //�������
    private final RequestProcessor nextProcessor; //��һ����������

    private Thread snapInProcess = null;//��������߳�
    volatile private boolean running; //�Ƿ�����

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     */
    private final LinkedList<Request> toFlush = new LinkedList<Request>();//�ȴ���ˢ�����̵��������
    private final Random r = new Random(System.nanoTime()); //�����
    /**
     * The number of log entries to log before starting a snapshot
     */
    private static int snapCount = ZooKeeperServer.getSnapCount(); //������Ŀ
    
    /**
     * The number of log entries before rolling the log, number
     * is chosen randomly
     */
    private static int randRoll; // һ������������������жϺ�ʱ��������־�ӵ�ǰ����������һ��

    private final Request requestOfDeath = Request.requestOfDeath; // ���������ʶ

    public SyncRequestProcessor(ZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }
    
    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
        randRoll = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }
    
    /**
     * Sets the value of randRoll. This method 
     * is here to avoid a findbugs warning for
     * setting a static variable in an instance
     * method. 
     * 
     * @param roll
     */
    private static void setRandRoll(int roll) {
        randRoll = roll;
    }

    @Override
    public void run() {
        try {
            int logCount = 0;

            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            setRandRoll(r.nextInt(snapCount/2));  //���������������
            while (true) {
                Request si = null;
                if (toFlush.isEmpty()) { //ˢ�µ����̵Ķ���Ϊ�� toFlush���ڵ����������һ���ж���������һ�����ύ
                    si = queuedRequests.take(); //������ȡ
                } else {
                    si = queuedRequests.poll();
                    if (si == null) {
                        flush(toFlush);
                        continue;
                    }
                }
                if (si == requestOfDeath) { //������ڽ�����־�������߳�
                    break;
                }
                if (si != null) { //����Ϊ��
                    // track the number of records written to the log
                    if (zks.getZKDatabase().append(si)) { //д��������־
                        logCount++;
                        if (logCount > (snapCount / 2 + randRoll)) { //logCount����һ������
                            randRoll = r.nextInt(snapCount/2); //��һ�����������ѡ��
                            // roll the log
                            zks.getZKDatabase().rollLog(); //�����µ���־�ļ�
                            // take a snapshot
                            if (snapInProcess != null && snapInProcess.isAlive()) { //���ڴ������
                                LOG.warn("Too busy to snap, skipping");
                            } else {
                                snapInProcess = new ZooKeeperThread("Snapshot Thread") { //���п���
                                        public void run() {
                                            try {
                                                zks.takeSnapshot();
                                            } catch(Exception e) {
                                                LOG.warn("Unexpected exception", e);
                                            }
                                        }
                                    };
                                snapInProcess.start(); //�����߳�����
                            }
                            logCount = 0; //��������
                        }
                    } else if (toFlush.isEmpty()) {//ˢ�µ����̵Ķ���Ϊ��
                        // optimization for read heavy workloads
                        // iff this is a read, and there are no pending
                        // flushes (writes), then just pass this to the next
                        // processor
                        if (nextProcessor != null) {
                            nextProcessor.processRequest(si); //��һ������������������
                            if (nextProcessor instanceof Flushable) {
                                ((Flushable)nextProcessor).flush(); //��һ����ˢ�̾�ˢ
                            }
                        }
                        continue;
                    }
                    toFlush.add(si); //��������ӽ���Ҫˢ����̶���
                    if (toFlush.size() > 1000) {
                        flush(toFlush); //����1000��ǿˢ����
                    }
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
            running = false;
            System.exit(11);
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    private void flush(LinkedList<Request> toFlush)
        throws IOException, RequestProcessorException
    {
        if (toFlush.isEmpty())
            return;

        zks.getZKDatabase().commit();
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();
            if (nextProcessor != null) {
                nextProcessor.processRequest(i);
            }
        }
        if (nextProcessor != null && nextProcessor instanceof Flushable) {
            ((Flushable)nextProcessor).flush();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(requestOfDeath);
        try {
            if(running){
                this.join();
            }
            if (!toFlush.isEmpty()) {
                flush(toFlush);
            }
        } catch(InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        queuedRequests.add(request);
    }

}
