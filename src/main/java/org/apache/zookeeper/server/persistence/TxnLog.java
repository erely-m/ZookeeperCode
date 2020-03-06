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

package org.apache.zookeeper.server.persistence;

import java.io.IOException;

import org.apache.jute.Record;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * Interface for reading transaction logs.
 *
 */
public interface TxnLog {
    
    /**
     * roll the current
     * log being appended to
     * @throws IOException 
     */
    void rollLog() throws IOException; //�ع���־
    /**
     * Append a request to the transaction log
     * @param hdr the transaction header
     * @param r the transaction itself
     * returns true iff something appended, otw false 
     * @throws IOException
     */
    boolean append(TxnHeader hdr, Record r) throws IOException;//׷����־

    /**
     * Start reading the transaction logs
     * from a given zxid
     * @param zxid
     * @return returns an iterator to read the 
     * next transaction in the logs.
     * @throws IOException
     */
    TxnIterator read(long zxid) throws IOException;//ͨ��zxid��ȡһ��������־
    
    /**
     * the last zxid of the logged transactions.
     * @return the last zxid of the logged transactions.
     * @throws IOException
     */
    long getLastLoggedZxid() throws IOException;//��ȡ���һ��zxid
    
    /**
     * truncate the log to get in sync with the 
     * leader.
     * @param zxid the zxid to truncate at.
     * @throws IOException 
     */
    boolean truncate(long zxid) throws IOException;//���zxid�����־
    
    /**
     * the dbid for this transaction log. 
     * @return the dbid for this transaction log.
     * @throws IOException
     */
    long getDbId() throws IOException;//��ȡ���ݿ�id
    
    /**
     * commmit the trasaction and make sure
     * they are persisted
     * @throws IOException
     */
    void commit() throws IOException;//�ύ����
   
    /** 
     * close the transactions logs
     */
    void close() throws IOException;//�ر�
    /**
     * an iterating interface for reading 
     * transaction logs. 
     */
    public interface TxnIterator { //�����ӿ�
        /**
         * return the transaction header.
         * @return return the transaction header.
         */
        TxnHeader getHeader();
        
        /**
         * return the transaction record.
         * @return return the transaction record.
         */
        Record getTxn(); //��ȡ�����¼
     
        /**
         * go to the next transaction record.
         * @throws IOException
         */
        boolean next() throws IOException; //��һ��
        
        /**
         * close files and release the 
         * resources
         * @throws IOException
         */
        void close() throws IOException; //�ر��ļ�
    }
}

