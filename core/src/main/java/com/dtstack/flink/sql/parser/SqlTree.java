/*
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

 

package com.dtstack.flink.sql.parser;


import com.dtstack.flink.sql.table.TableInfo;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * 解析sql获得的对象结构
 * Date: 2018/6/25
 * Company: www.dtstack.com
 * @author xuchao
 */

public class SqlTree {

    private List<CreateFuncParser.SqlParserResult> functionList = Lists.newArrayList();

    private Map<String, CreateTableParser.SqlParserResult> preDealTableMap = Maps.newHashMap();

    //存储了sql语句中涉及的所有输入表，输出表，维表的名称、字段、字段类型、连接信息
    private Map<String, TableInfo> tableInfoMap = Maps.newLinkedHashMap();

    private List<InsertSqlParser.SqlParseResult> execSqlList = Lists.newArrayList();

    private List<CreateTmpTableParser.SqlParserResult> tmpSqlList = Lists.newArrayList();

    private Map<String, CreateTmpTableParser.SqlParserResult> tmpTableMap = Maps.newHashMap();

    public List<CreateFuncParser.SqlParserResult> getFunctionList() {
        return functionList;
    }

    public Map<String, CreateTableParser.SqlParserResult> getPreDealTableMap() {
        return preDealTableMap;
    }

    public Map<String, CreateTmpTableParser.SqlParserResult> getTmpTableMap() {
        return tmpTableMap;
    }

    public List<InsertSqlParser.SqlParseResult> getExecSqlList() {
        return execSqlList;
    }

    public void addFunc(CreateFuncParser.SqlParserResult func){
        functionList.add(func);
    }

    public void addPreDealTableInfo(String tableName, CreateTableParser.SqlParserResult table){
        preDealTableMap.put(tableName, table);
    }

    public void addTmplTableInfo(String tableName, CreateTmpTableParser.SqlParserResult table){
        tmpTableMap.put(tableName, table);
    }

    public void addExecSql(InsertSqlParser.SqlParseResult execSql){
        execSqlList.add(execSql);
    }

    public void addTmpSql(CreateTmpTableParser.SqlParserResult tmpSql){
        tmpSqlList.add(tmpSql);
    }

    public List<CreateTmpTableParser.SqlParserResult> getTmpSqlList(){
        return tmpSqlList;
    }

    public Map<String, TableInfo> getTableInfoMap() {
        return tableInfoMap;
    }

    public void addTableInfo(String tableName, TableInfo tableInfo){
        tableInfoMap.put(tableName, tableInfo);
    }


    // 重写 toString
    @Override
    public String toString(){

        StringBuffer strBuffer = new StringBuffer();
        for (int i = 0; i < functionList.size(); i++) {
            strBuffer.append("functionList: " + functionList.get(i).getName() + "," + functionList.get(i).getClassName() + "," + functionList.get(i).getType());
            strBuffer.append(",");
        }
        strBuffer.append("/r/n");

        for (int i = 0; i < execSqlList.size(); i++) {
            strBuffer.append("execSqlList: " + execSqlList.get(i).getSourceTableList() + "," + execSqlList.get(i).getTargetTableList() + "," + execSqlList.get(i).getExecSql());
            strBuffer.append(",");
        }
        strBuffer.append("/r/n");


        strBuffer.append("preDealTableMap： ");
        for (Map.Entry<String, CreateTableParser.SqlParserResult> entry : preDealTableMap.entrySet()) {
            strBuffer.append("Key = " + entry.getKey() + ", Value = " + entry.getValue());
            strBuffer.append(",");
        }
        strBuffer.append("/r/n");


        strBuffer.append("tableInfoMap： ");
        for (Map.Entry<String, TableInfo> entry : tableInfoMap.entrySet()) {
            strBuffer.append("Key = " + entry.getKey() + ", Value = " + entry.getValue());
            strBuffer.append(",");
        }
        strBuffer.append("/r/n");

        return strBuffer.toString();
    }
}
