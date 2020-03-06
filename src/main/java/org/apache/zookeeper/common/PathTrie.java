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

package org.apache.zookeeper.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a class that implements prefix matching for 
 * components of a filesystem path. the trie
 * looks like a tree with edges mapping to 
 * the component of a path.
 * example /ab/bc/cf would map to a trie
 *           /
 *        ab/
 *        (ab)
 *      bc/
 *       / 
 *      (bc)
 *   cf/
 *   (cf)
 */
//字典树的实现 父节点下面是子节点path
public class PathTrie {
    /**
     * the logger for this class
     */
    private static final Logger LOG = LoggerFactory.getLogger(PathTrie.class);
    
    /**
     * the root node of PathTrie
     */
    private final TrieNode rootNode ;
    
    static class TrieNode {
        boolean property = false; //是否配额
        final HashMap<String, TrieNode> children; //记录孩子节点和path
        TrieNode parent = null; //父节点
        /**
         * create a trienode with parent
         * as parameter
         * @param parent the parent of this trienode
         */
        private TrieNode(TrieNode parent) { //构造节点
            children = new HashMap<String, TrieNode>();
            this.parent = parent;
        }
        
        /**
         * get the parent of this node
         * @return the parent node
         */
        TrieNode getParent() {
            return this.parent;
        }
        
        /**
         * set the parent of this node
         * @param parent the parent to set to
         */
        void setParent(TrieNode parent) {
            this.parent = parent;
        }
        
        /**
         * a property that is set 
         * for a node - making it 
         * special.
         */
        void setProperty(boolean prop) {
            this.property = prop;
        }
        
        /** the property of this
         * node 
         * @return the property for this
         * node
         */
        boolean getProperty() {
            return this.property;
        }
        /**
         * add a child to the existing node
         * @param childName the string name of the child
         * @param node the node that is the child
         */
        void addChild(String childName, TrieNode node) {//添加孩子节点信息
            synchronized(children) {
                if (children.containsKey(childName)) {
                    return;
                }
                children.put(childName, node);
            }
        }
     
        /**
         * delete child from this node
         * @param childName the string name of the child to 
         * be deleted
         */
        void deleteChild(String childName) { //删除孩子节点信息
            synchronized(children) {
                if (!children.containsKey(childName)) {
                    return;
                }
                TrieNode childNode = children.get(childName);
                // this is the only child node.
                if (childNode.getChildren().length == 1) { 
                    childNode.setParent(null);
                    children.remove(childName);
                }
                else {
                    // their are more child nodes
                    // so just reset property.
                    childNode.setProperty(false);
                }
            }
        }
        
        /**
         * return the child of a node mapping
         * to the input childname
         * @param childName the name of the child
         * @return the child of a node
         */
        TrieNode getChild(String childName) { //获取孩子节点
            synchronized(children) {
               if (!children.containsKey(childName)) {
                   return null;
               }
               else {
                   return children.get(childName);
               }
            }
        }

        /**
         * get the list of children of this 
         * trienode.
         * @param node to get its children
         * @return the string list of its children
         */
        String[] getChildren() {//获取孩子节点的名称
           synchronized(children) {
               return children.keySet().toArray(new String[0]);
           }
        }
        
        /**
         * get the string representation
         * for this node
         */
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Children of trienode: ");
            synchronized(children) {
                for (String str: children.keySet()) {
                    sb.append(" " + str);
                }
            }
            return sb.toString();
        }
    }
    
    /**
     * construct a new PathTrie with
     * a root node of /
     */
    public PathTrie() {
        this.rootNode = new TrieNode(null); //创建字典树
    }
    
    /**
     * add a path to the path trie 
     * @param path
     */
    public void addPath(String path) {
        if (path == null) {
            return;
        }
        String[] pathComponents = path.split("/"); //节点path拆分
        TrieNode parent = rootNode; //
        String part = null;
        if (pathComponents.length <= 1) { //如果拆分后长度小于1直接抛出异常
            throw new IllegalArgumentException("Invalid path " + path);
        }
        for (int i=1; i<pathComponents.length; i++) {  //循环遍历整个所有的节点，
            part = pathComponents[i];
            if (parent.getChild(part) == null) { //如果对应父节点的孩子节点的字典树为空则添加对应的字典树
                parent.addChild(part, new TrieNode(parent));//添加字典树节点
            }
            parent = parent.getChild(part); //重新赋值父节点
        }
        parent.setProperty(true); //设置配额属性
    }
    
    /**
     * delete a path from the trie
     * @param path the path to be deleted
     */
    public void deletePath(String path) {//删除一个path
        if (path == null) {
            return;
        }
        String[] pathComponents = path.split("/");
        TrieNode parent = rootNode;
        String part = null;
        if (pathComponents.length <= 1) { 
            throw new IllegalArgumentException("Invalid path " + path);
        }
        for (int i=1; i<pathComponents.length; i++) {
            part = pathComponents[i];
            if (parent.getChild(part) == null) { //如果不存在子节点直接返回
                //the path does not exist 
                return;
            }
            parent = parent.getChild(part);
            LOG.info("{}",parent);
        }
        TrieNode realParent  = parent.getParent();//获取当前节点的父节点
        realParent.deleteChild(part); //删除当前节点
    }
    
    /**
     * return the largest prefix for the input path.
     * @param path the input path
     * @return the largest prefix for the input path.
     */
    public String findMaxPrefix(String path) { //找到一个最近的拥有配额标记的祖先节点
        if (path == null) {
            return null;
        }
        if ("/".equals(path)) {
            return path;
        }
        String[] pathComponents = path.split("/"); //拆分path
        TrieNode parent = rootNode;
        List<String> components = new ArrayList<String>();
        if (pathComponents.length <= 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }
        int i = 1; //从1开始因为路径都是"/"开头的,pathComponents[0]会是""
        String part = null;
        StringBuilder sb = new StringBuilder();
        int lastindex = -1;
        while((i < pathComponents.length)) {
            if (parent.getChild(pathComponents[i]) != null) {
                part = pathComponents[i];
                parent = parent.getChild(part); //当前节点
                components.add(part);//当前节点路径加入到List中
                if (parent.getProperty()) { //如果孩子节点有配置
                    lastindex = i-1; //更新最后一个有标记的节点(也就是最近的有标记的祖先)
                }
            }
            else {
                break;
            }
            i++;
        }
        for (int j=0; j< (lastindex+1); j++) {
            sb.append("/" + components.get(j)); //拼接节点
        }
        return sb.toString();
    }

    /**
     * clear all nodes
     */
    public void clear() {
        for(String child : rootNode.getChildren()) {
            rootNode.deleteChild(child);
        }
    }
}
