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
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.util.SerializeUtils;

/**
 * This class implements the snapshot interface.
 * it is responsible for storing, serializing
 * and deserializing the right snapshot.
 * and provides access to the snapshots.
 */
public class FileSnap implements SnapShot {
    File snapDir; //快照目录
    private volatile boolean close = false; //关闭标志
    private static final int VERSION=2;//版本号
    private static final long dbId=-1;//db的Id
    private static final Logger LOG = LoggerFactory.getLogger(FileSnap.class);
    public final static int SNAP_MAGIC
        = ByteBuffer.wrap("ZKSN".getBytes()).getInt(); //快照魔数
    public FileSnap(File snapDir) {
        this.snapDir = snapDir;
    }

    /**
     * deserialize a data tree from the most recent snapshot
     * @return the zxid of the snapshot
     */ 
    public long deserialize(DataTree dt, Map<Long, Integer> sessions)
            throws IOException {
        // we run through 100 snapshots (not all of them)
        // if we cannot get it running within 100 snapshots
        // we should  give up
        List<File> snapList = findNValidSnapshots(100); //可反序列的文件列表最多100个
        if (snapList.size() == 0) {
            return -1L;
        }
        File snap = null;
        boolean foundValid = false;
        for (int i = 0; i < snapList.size(); i++) { //取出第一个进行反序列化 从新到旧排序
            snap = snapList.get(i);
            InputStream snapIS = null;
            CheckedInputStream crcIn = null;
            try {
                LOG.info("Reading snapshot " + snap);
                snapIS = new BufferedInputStream(new FileInputStream(snap));
                crcIn = new CheckedInputStream(snapIS, new Adler32());
                InputArchive ia = BinaryInputArchive.getArchive(crcIn);
                deserialize(dt,sessions, ia); //反序列化
                long checkSum = crcIn.getChecksum().getValue(); //计算检查数
                long val = ia.readLong("val");
                if (val != checkSum) { //校验检查数
                    throw new IOException("CRC corruption in snapshot :  " + snap);
                }
                foundValid = true;//反序列化完成结束循环（有一个完成就ok）
                break;
            } catch(IOException e) {
                LOG.warn("problem reading snap file " + snap, e);
            } finally {
                if (snapIS != null) 
                    snapIS.close();
                if (crcIn != null) 
                    crcIn.close();
            } 
        }
        if (!foundValid) {
            throw new IOException("Not able to find valid snapshots in " + snapDir);
        }
        //从快照文件中获取最后的一个zxid并返回 次数内存数据库根据数据快照已经恢复了一部分 剩余的一部分需要通过事务日志进行恢复 通过zxid对事务日志进行恢复
        dt.lastProcessedZxid = Util.getZxidFromName(snap.getName(), "snapshot");
        return dt.lastProcessedZxid;
    }

    /**
     * deserialize the datatree from an inputarchive
     * @param dt the datatree to be serialized into
     * @param sessions the sessions to be filled up
     * @param ia the input archive to restore from
     * @throws IOException
     */
    public void deserialize(DataTree dt, Map<Long, Integer> sessions,
            InputArchive ia) throws IOException {
        FileHeader header = new FileHeader();
        header.deserialize(ia, "fileheader");//反序列化值header（FileHeader 文件头）
        if (header.getMagic() != SNAP_MAGIC) {
            throw new IOException("mismatching magic headers "
                    + header.getMagic() + 
                    " !=  " + FileSnap.SNAP_MAGIC);
        }
        SerializeUtils.deserializeSnapshot(dt,ia,sessions); //反序列化快照至DT（DataTree） 和 sessions
    }

    /**
     * find the most recent snapshot in the database.
     * @return the file containing the most recent snapshot
     */
    public File findMostRecentSnapshot() throws IOException {
        List<File> files = findNValidSnapshots(1); //找到最近的一个快照
        if (files.size() == 0) {
            return null;
        }
        return files.get(0);
    }
    
    /**
     * find the last (maybe) valid n snapshots. this does some 
     * minor checks on the validity of the snapshots. It just
     * checks for / at the end of the snapshot. This does
     * not mean that the snapshot is truly valid but is
     * valid with a high probability. also, the most recent 
     * will be first on the list. 
     * @param n the number of most recent snapshots
     * @return the last n snapshots (the number might be
     * less than n in case enough snapshots are not available).
     * @throws IOException
     */
    private List<File> findNValidSnapshots(int n) throws IOException {//寻找合理的快照
        List<File> files = Util.sortDataDir(snapDir.listFiles(),"snapshot", false);//对文件进行排序降序
        int count = 0;
        List<File> list = new ArrayList<File>();
        for (File f : files) {
            // we should catch the exceptions
            // from the valid snapshot and continue
            // until we find a valid one
            try {
                if (Util.isValidSnapshot(f)) {//检查快照是否合理
                    list.add(f);
                    count++;
                    if (count == n) {
                        break;
                    }
                }
            } catch (IOException e) {
                LOG.info("invalid snapshot " + f, e);
            }
        }
        return list;
    }

    /**
     * find the last n snapshots. this does not have
     * any checks if the snapshot might be valid or not
     * @param the number of most recent snapshots 
     * @return the last n snapshots
     * @throws IOException
     */
    public List<File> findNRecentSnapshots(int n) throws IOException {//找到最近的n个快照文件
        List<File> files = Util.sortDataDir(snapDir.listFiles(), "snapshot", false); //对文件进行排序 找到最后几个需要保存的快照
        int i = 0;
        List<File> list = new ArrayList<File>();
        for (File f: files) {
            if (i==n)
                break;
            i++;
            list.add(f);
        }
        return list;
    }

    /**
     * serialize the datatree and sessions
     * @param dt the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param oa the output archive to serialize into
     * @param header the header of this snapshot
     * @throws IOException
     */
    protected void serialize(DataTree dt,Map<Long, Integer> sessions,
            OutputArchive oa, FileHeader header) throws IOException {
        // this is really a programmatic error and not something that can
        // happen at runtime
        if(header==null)
            throw new IllegalStateException(
                    "Snapshot's not open for writing: uninitialized header");
        header.serialize(oa, "fileheader"); //序列化header
        SerializeUtils.serializeSnapshot(dt,oa,sessions); //序列化DT Sessions
    }

    /**
     * serialize the datatree and session into the file snapshot
     * @param dt the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param snapShot the file to store snapshot into
     */
    public synchronized void serialize(DataTree dt, Map<Long, Integer> sessions, File snapShot)
            throws IOException {
        if (!close) { //如果文件流未关闭
            OutputStream sessOS = new BufferedOutputStream(new FileOutputStream(snapShot)); //创建快照文件输出流
            CheckedOutputStream crcOut = new CheckedOutputStream(sessOS, new Adler32()); //检查
            //CheckedOutputStream cout = new CheckedOutputStream()
            OutputArchive oa = BinaryOutputArchive.getArchive(crcOut);
            FileHeader header = new FileHeader(SNAP_MAGIC, VERSION, dbId);
            serialize(dt,sessions,oa, header);//将dt、sessions进行序列化
            long val = crcOut.getChecksum().getValue(); //checksum
            oa.writeLong(val, "val"); //write checksum to file
            oa.writeString("/", "path");//写入序列化的内容
            sessOS.flush();
            crcOut.close();
            sessOS.close();
        }
    }

    /**
     * synchronized close just so that if serialize is in place
     * the close operation will block and will wait till serialize
     * is done and will set the close flag
     */
    @Override
    public synchronized void close() throws IOException { //关闭资源
        close = true;
    }

 }
