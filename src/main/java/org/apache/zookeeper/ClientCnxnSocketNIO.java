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

package org.apache.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.zookeeper.ClientCnxn.EndOfStreamException;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientCnxnSocketNIO extends ClientCnxnSocket {
    private static final Logger LOG = LoggerFactory
            .getLogger(ClientCnxnSocketNIO.class);

    private final Selector selector = Selector.open(); //打开选择器

    private SelectionKey sockKey; //事件Key

    ClientCnxnSocketNIO() throws IOException {
        super();
    }

    @Override
    boolean isConnected() { //是否连接
        return sockKey != null;
    }
    
    /**
     * @return true if a packet was received
     * @throws InterruptedException
     * @throws IOException
     */
    void doIO(List<Packet> pendingQueue, LinkedList<Packet> outgoingQueue, ClientCnxn cnxn)
      throws InterruptedException, IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel(); //SocketChannel
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        if (sockKey.isReadable()) { //如果可读就绪
            int rc = sock.read(incomingBuffer); //读取数据
            if (rc < 0) {
                throw new EndOfStreamException(
                        "Unable to read additional data from server sessionid 0x"
                                + Long.toHexString(sessionId)
                                + ", likely server has closed socket");
            }
            if (!incomingBuffer.hasRemaining()) { //如果还有数据
                incomingBuffer.flip(); //切换到读模式
                if (incomingBuffer == lenBuffer) { //如果读取是4位长度
                    recvCount++;  //接收次数加1
                    readLength(); //读取长度并分配空间
                } else if (!initialized) { //如果还没有初始化
                    readConnectResult(); //读取connect的响应
                    enableRead(); //设置可读
                    if (findSendablePacket(outgoingQueue,
                            cnxn.sendThread.clientTunneledAuthenticationInProgress()) != null) {
                        // Since SASL authentication has completed (if client is configured to do so),
                        // outgoing packets waiting in the outgoingQueue can now be sent.
                        enableWrite(); //可写
                    }
                    lenBuffer.clear(); //
                    incomingBuffer = lenBuffer; //还原incomingBuffer为读长度头
                    updateLastHeard(); //更新监听时间
                    initialized = true; //初始化完成
                } else {
                    sendThread.readResponse(incomingBuffer); //从incomingBuffer中读取响应数据
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer;//还原incomingBuffer为读长度头
                    updateLastHeard();//更新监听时间
                }
            }
        }
        if (sockKey.isWritable()) { //如果写就绪
            synchronized(outgoingQueue) {
                Packet p = findSendablePacket(outgoingQueue,
                        cnxn.sendThread.clientTunneledAuthenticationInProgress()); //找到可以发送的数据包

                if (p != null) {
                    updateLastSend(); //更新最后发送时间
                    // If we already started writing p, p.bb will already exist
                    if (p.bb == null) { //如果byteBuffer为空
                        if ((p.requestHeader != null) &&
                                (p.requestHeader.getType() != OpCode.ping) &&
                                (p.requestHeader.getType() != OpCode.auth)) {
                            p.requestHeader.setXid(cnxn.getXid()); //设置请求序号
                        }
                        p.createBB(); //创建byteBuffer
                    }
                    sock.write(p.bb); //将数据写入bb中
                    if (!p.bb.hasRemaining()) { //如果不可写了
                        sentCount++; //发送次数加1
                        outgoingQueue.removeFirstOccurrence(p); //从待发送队列中取出该packet
                        if (p.requestHeader != null
                                && p.requestHeader.getType() != OpCode.ping
                                && p.requestHeader.getType() != OpCode.auth) {
                            synchronized (pendingQueue) {
                                pendingQueue.add(p); ////加入待回复的队列
                            }
                        }
                    }
                }
                if (outgoingQueue.isEmpty()) { //发送队列没有数据可以发送了
                    // No more packets to send: turn off write interest flag.
                    // Will be turned on later by a later call to enableWrite(),
                    // from within ZooKeeperSaslClient (if client is configured
                    // to attempt SASL authentication), or in either doIO() or
                    // in doTransport() if not.
                    disableWrite(); //不可写
                } else if (!initialized && p != null && !p.bb.hasRemaining()) { //未初始化，数据包为空
                    // On initial connection, write the complete connect request
                    // packet, but then disable further writes until after
                    // receiving a successful connection response.  If the
                    // session is expired, then the server sends the expiration
                    // response and immediately closes its end of the socket.  If
                    // the client is simultaneously writing on its end, then the
                    // TCP stack may choose to abort with RST, in which case the
                    // client would never receive the session expired event.  See
                    // http://docs.oracle.com/javase/6/docs/technotes/guides/net/articles/connection_release.html
                    disableWrite(); //不可写
                } else {
                    // Just in case
                    enableWrite(); //可写
                }
            }
        }
    }

    private Packet findSendablePacket(LinkedList<Packet> outgoingQueue,
                                      boolean clientTunneledAuthenticationInProgress) {
        synchronized (outgoingQueue) {
            if (outgoingQueue.isEmpty()) {
                return null;
            }
            if (outgoingQueue.getFirst().bb != null // If we've already starting sending the first packet, we better finish
                || !clientTunneledAuthenticationInProgress) { //如果队列数据为不为空或者没有正在处理权限的
                return outgoingQueue.getFirst(); //如果队列中有数据将数据出队
            }

            // Since client's authentication with server is in progress,
            // send only the null-header packet queued by primeConnection().
            // This packet must be sent so that the SASL authentication process
            // can proceed, but all other packets should wait until
            // SASL authentication completes.
            ListIterator<Packet> iter = outgoingQueue.listIterator();
            while (iter.hasNext()) {
                Packet p = iter.next();
                if (p.requestHeader == null) {////如果在处理sasl的权限，那么只有requestHeader为null的Packet可以被发送
                    // We've found the priming-packet. Move it to the beginning of the queue.
                    iter.remove();
                    outgoingQueue.add(0, p);
                    return p;
                } else {
                    // Non-priming packet: defer it until later, leaving it in the queue
                    // until authentication completes.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("deferring non-priming packet: " + p +
                                "until SASL authentication completes.");
                    }
                }
            }
            // no sendable packet found.
            return null;
        }
    }

    @Override
    void cleanup() {
        if (sockKey != null) {
            SocketChannel sock = (SocketChannel) sockKey.channel();
            sockKey.cancel();
            try {
                sock.socket().shutdownInput();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during shutdown input", e);
                }
            }
            try {
                sock.socket().shutdownOutput();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during shutdown output",
                            e);
                }
            }
            try {
                sock.socket().close();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during socket close", e);
                }
            }
            try {
                sock.close();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during channel close", e);
                }
            }
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SendThread interrupted during sleep, ignoring");
            }
        }
        sockKey = null;
    }
 
    @Override
    void close() {
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Doing client selector close");
            }
            selector.close();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Closed client selector");
            }
        } catch (IOException e) {
            LOG.warn("Ignoring exception during selector close", e);
        }
    }
    
    /**
     * create a socket channel.
     * @return the created socket channel
     * @throws IOException
     */
    SocketChannel createSock() throws IOException {
        SocketChannel sock;
        sock = SocketChannel.open();
        sock.configureBlocking(false); //非堵塞
        sock.socket().setSoLinger(false, -1);
        sock.socket().setTcpNoDelay(true);
        return sock;
    }

    /**
     * register with the selection and connect
     * @param sock the {@link SocketChannel} 
     * @param addr the address of remote host
     * @throws IOException
     */
    void registerAndConnect(SocketChannel sock, InetSocketAddress addr) 
    throws IOException {
        sockKey = sock.register(selector, SelectionKey.OP_CONNECT); //注册，监听connect时间
        boolean immediateConnect = sock.connect(addr);
        if (immediateConnect) { //如果立即建立了连接
            sendThread.primeConnection();//client把watches和authData等数据发过去，并更新SelectionKey为读写
        }
    }
    
    @Override
    void connect(InetSocketAddress addr) throws IOException { //参数是某一个zk server的地址
        SocketChannel sock = createSock();
        try {
           registerAndConnect(sock, addr); //注册和建立连接
        } catch (IOException e) {
            LOG.error("Unable to open socket to " + addr);
            sock.close();
            throw e;
        }
        initialized = false; //还没有初始化,connect ok了但是还读到server的response

        /*
         * Reset incomingBuffer
         */
        lenBuffer.clear(); //清空lenBuffer
        incomingBuffer = lenBuffer;
    }

    /**
     * Returns the address to which the socket is connected.
     * 
     * @return ip address of the remote side of the connection or null if not
     *         connected
     */
    @Override
    SocketAddress getRemoteSocketAddress() {
        // a lot could go wrong here, so rather than put in a bunch of code
        // to check for nulls all down the chain let's do it the simple
        // yet bulletproof way
        try {
            return ((SocketChannel) sockKey.channel()).socket()
                    .getRemoteSocketAddress();
        } catch (NullPointerException e) {
            return null;
        }
    }

    /**
     * Returns the local address to which the socket is bound.
     * 
     * @return ip address of the remote side of the connection or null if not
     *         connected
     */
    @Override
    SocketAddress getLocalSocketAddress() {
        // a lot could go wrong here, so rather than put in a bunch of code
        // to check for nulls all down the chain let's do it the simple
        // yet bulletproof way
        try {
            return ((SocketChannel) sockKey.channel()).socket()
                    .getLocalSocketAddress();
        } catch (NullPointerException e) {
            return null;
        }
    }

    @Override
    synchronized void wakeupCnxn() {
        selector.wakeup();
    }
    
    @Override
    void doTransport(int waitTimeOut, List<Packet> pendingQueue, LinkedList<Packet> outgoingQueue,
                     ClientCnxn cnxn)
            throws IOException, InterruptedException {
        selector.select(waitTimeOut); //定时处理时间
        Set<SelectionKey> selected;
        synchronized (this) {
            selected = selector.selectedKeys(); //获取所有触发事件
        }
        // Everything below and until we get back to the select is
        // non blocking, so time is effectively a constant. That is
        // Why we just have to do this once, here
        updateNow(); //更新对应时间
        for (SelectionKey k : selected) { //遍历事件
            SocketChannel sc = ((SocketChannel) k.channel());
            if ((k.readyOps() & SelectionKey.OP_CONNECT) != 0) { //如果就绪时间是connect 这个出现在registerAndConnect函数没有立即连接成功
                if (sc.finishConnect()) {
                    updateLastSendAndHeard();//更新连接时间
                    sendThread.primeConnection(); //client把watches和authData等数据发过去，并更新SelectionKey为读写
                }
            } else if ((k.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) { //如果为读或者写就绪说明已经连接成功了进行IO
                doIO(pendingQueue, outgoingQueue, cnxn);//利用pendingQueue和outgoingQueue进行IO
            }
        }
        if (sendThread.getZkState().isConnected()) { //如果zk的state是已连接
            synchronized(outgoingQueue) {
                if (findSendablePacket(outgoingQueue,
                        cnxn.sendThread.clientTunneledAuthenticationInProgress()) != null) { //存在待发送的数据包
                    enableWrite();//注册写事件
                }
            }
        }
        selected.clear(); //清空
    }

    //TODO should this be synchronized?
    @Override
    void testableCloseSocket() throws IOException {
        LOG.info("testableCloseSocket() called");
        ((SocketChannel) sockKey.channel()).socket().close();
    }

    @Override
    synchronized void enableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_WRITE);
        }
    }

    @Override
    public synchronized void disableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) != 0) {
            sockKey.interestOps(i & (~SelectionKey.OP_WRITE));
        }
    }

    synchronized private void enableRead() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_READ) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_READ);
        }
    }

    @Override
    synchronized void enableReadWriteOnly() {
        sockKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    Selector getSelector() {
        return selector;
    }

    @Override
    void sendPacket(Packet p) throws IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel();
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        p.createBB();
        ByteBuffer pbb = p.bb;
        sock.write(pbb);
    }


}
