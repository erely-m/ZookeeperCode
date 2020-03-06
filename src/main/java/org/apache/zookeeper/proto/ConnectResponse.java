// File generated by hadoop record compiler. Do not edit.
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

package org.apache.zookeeper.proto;

import org.apache.jute.*;
public class ConnectResponse implements Record {
  private int protocolVersion; //协议版本
  private int timeOut; //超时时间
  private long sessionId;//SessionId
  private byte[] passwd;//密码
  public ConnectResponse() {
  }
  public ConnectResponse(
        int protocolVersion,
        int timeOut,
        long sessionId,
        byte[] passwd) {
    this.protocolVersion=protocolVersion;
    this.timeOut=timeOut;
    this.sessionId=sessionId;
    this.passwd=passwd;
  }
  public int getProtocolVersion() {
    return protocolVersion;
  }
  public void setProtocolVersion(int m_) {
    protocolVersion=m_;
  }
  public int getTimeOut() {
    return timeOut;
  }
  public void setTimeOut(int m_) {
    timeOut=m_;
  }
  public long getSessionId() {
    return sessionId;
  }
  public void setSessionId(long m_) {
    sessionId=m_;
  }
  public byte[] getPasswd() {
    return passwd;
  }
  public void setPasswd(byte[] m_) {
    passwd=m_;
  }
  public void serialize(OutputArchive a_, String tag) throws java.io.IOException {
    a_.startRecord(this,tag);
    a_.writeInt(protocolVersion,"protocolVersion");
    a_.writeInt(timeOut,"timeOut");
    a_.writeLong(sessionId,"sessionId");
    a_.writeBuffer(passwd,"passwd");
    a_.endRecord(this,tag);
  }
  public void deserialize(InputArchive a_, String tag) throws java.io.IOException {
    a_.startRecord(tag);
    protocolVersion=a_.readInt("protocolVersion");
    timeOut=a_.readInt("timeOut");
    sessionId=a_.readLong("sessionId");
    passwd=a_.readBuffer("passwd");
    a_.endRecord(tag);
}
  public String toString() {
    try {
      java.io.ByteArrayOutputStream s =
        new java.io.ByteArrayOutputStream();
      CsvOutputArchive a_ = 
        new CsvOutputArchive(s);
      a_.startRecord(this,"");
    a_.writeInt(protocolVersion,"protocolVersion");
    a_.writeInt(timeOut,"timeOut");
    a_.writeLong(sessionId,"sessionId");
    a_.writeBuffer(passwd,"passwd");
      a_.endRecord(this,"");
      return new String(s.toByteArray(), "UTF-8");
    } catch (Throwable ex) {
      ex.printStackTrace();
    }
    return "ERROR";
  }
  public void write(java.io.DataOutput out) throws java.io.IOException {
    BinaryOutputArchive archive = new BinaryOutputArchive(out);
    serialize(archive, "");
  }
  public void readFields(java.io.DataInput in) throws java.io.IOException {
    BinaryInputArchive archive = new BinaryInputArchive(in);
    deserialize(archive, "");
  }
  public int compareTo (Object peer_) throws ClassCastException {
    if (!(peer_ instanceof ConnectResponse)) {
      throw new ClassCastException("Comparing different types of records.");
    }
    ConnectResponse peer = (ConnectResponse) peer_;
    int ret = 0;
    ret = (protocolVersion == peer.protocolVersion)? 0 :((protocolVersion<peer.protocolVersion)?-1:1);
    if (ret != 0) return ret;
    ret = (timeOut == peer.timeOut)? 0 :((timeOut<peer.timeOut)?-1:1);
    if (ret != 0) return ret;
    ret = (sessionId == peer.sessionId)? 0 :((sessionId<peer.sessionId)?-1:1);
    if (ret != 0) return ret;
    {
      byte[] my = passwd;
      byte[] ur = peer.passwd;
      ret = org.apache.jute.Utils.compareBytes(my,0,my.length,ur,0,ur.length);
    }
    if (ret != 0) return ret;
     return ret;
  }
  public boolean equals(Object peer_) {
    if (!(peer_ instanceof ConnectResponse)) {
      return false;
    }
    if (peer_ == this) {
      return true;
    }
    ConnectResponse peer = (ConnectResponse) peer_;
    boolean ret = false;
    ret = (protocolVersion==peer.protocolVersion);
    if (!ret) return ret;
    ret = (timeOut==peer.timeOut);
    if (!ret) return ret;
    ret = (sessionId==peer.sessionId);
    if (!ret) return ret;
    ret = org.apache.jute.Utils.bufEquals(passwd,peer.passwd);
    if (!ret) return ret;
     return ret;
  }
  public int hashCode() {
    int result = 17;
    int ret;
    ret = (int)protocolVersion;
    result = 37*result + ret;
    ret = (int)timeOut;
    result = 37*result + ret;
    ret = (int) (sessionId^(sessionId>>>32));
    result = 37*result + ret;
    ret = java.util.Arrays.toString(passwd).hashCode();
    result = 37*result + ret;
    return result;
  }
  public static String signature() {
    return "LConnectResponse(iilB)";
  }
}
