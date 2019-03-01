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

 

package com.dtstack.flink.sql.table;

import com.dtstack.flink.sql.enums.ETableType;
import com.dtstack.flink.sql.parser.CreateTableParser;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.StreamSideFactory;
import com.dtstack.flink.sql.sink.StreamSinkFactory;
import com.dtstack.flink.sql.source.StreamSourceFactory;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Maps;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Create table statement parsing table structure to obtain specific information
 * Date: 2018/6/25
 * Company: www.dtstack.com
 * @author xuchao
 */

public class TableInfoParser {

    private final static String TYPE_KEY = "type";

    private final static String SIDE_TABLE_SIGN = "(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$";

    private final static Pattern SIDE_PATTERN = Pattern.compile(SIDE_TABLE_SIGN);

    // 用于存储所有支持的输入表类型：<kafka09, KafkaSourceParser>  <kafka10, KafkaSourceParser> ...
    // 继承关系：AbsTableParser -> AbsSourceParser -> KafkaSourceParser（各个source工程中）
    private  Map<String, AbsTableParser> sourceTableInfoMap = Maps.newConcurrentMap();

    // 用于存储所有支持的输出表类型：<hbase, HbaseSinkParser>  ...
    // 继承关系: AbsTableParser -> HbaseSinkParser（各个sink工程中）
    private  Map<String, AbsTableParser> targetTableInfoMap = Maps.newConcurrentMap();

    // 用于存储所有支持的维表类型：<hbase, HbaseSideParser> ...
    // 继承关系: AbsTableParser -> AbsSideTableParser -> HbaseSideParser
    private  Map<String, AbsTableParser> sideTableInfoMap = Maps.newConcurrentMap();

    //Parsing loaded plugin
    public TableInfo parseWithTableType(int tableType, CreateTableParser.SqlParserResult parserResult,
                                               String localPluginRoot) throws Exception {
        AbsTableParser absTableParser = null;
        Map<String, Object> props = parserResult.getPropMap();
        String type = MathUtil.getString(props.get(TYPE_KEY));

        if(Strings.isNullOrEmpty(type)){
            throw new RuntimeException("create table statement requires property of type");
        }

        if(tableType == ETableType.SOURCE.getType()){
            // 判断输入表是不是维表
            boolean isSideTable = checkIsSideTable(parserResult.getFieldsInfoStr());

            if(!isSideTable){
                // 输入表不是维表
                absTableParser = sourceTableInfoMap.get(type);
                // 将所有支持的输入表类型存入sourceTableInfoMap，
                if(absTableParser == null){
                    //
                    absTableParser = StreamSourceFactory.getSqlParser(type, localPluginRoot);
                    sourceTableInfoMap.put(type, absTableParser);
                }
            }else{
                // 输入表是维表
                absTableParser = sideTableInfoMap.get(type);
                if(absTableParser == null){
                    String cacheType = MathUtil.getString(props.get(SideTableInfo.CACHE_KEY));
                    absTableParser = StreamSideFactory.getSqlParser(type, localPluginRoot, cacheType);
                    sideTableInfoMap.put(type, absTableParser);
                }
            }

        }else if(tableType == ETableType.SINK.getType()){
            absTableParser = targetTableInfoMap.get(type);
            if(absTableParser == null){
                absTableParser = StreamSinkFactory.getSqlParser(type, localPluginRoot);
                targetTableInfoMap.put(type, absTableParser);
            }
        }

        if(absTableParser == null){
            throw new RuntimeException(String.format("not support %s type of table", type));
        }

        Map<String, Object> prop = Maps.newHashMap();

        //Shield case
        parserResult.getPropMap().forEach((key,val) -> prop.put(key.toLowerCase(), val));

        return absTableParser.getTableInfo(parserResult.getTableName(), parserResult.getFieldsInfoStr(), prop);
    }

    /**
     * judge dim table of PERIOD FOR SYSTEM_TIME
     * @param tableField
     * @return
     */
    private static boolean checkIsSideTable(String tableField){
        String[] fieldInfos = tableField.split(",");
        for(String field : fieldInfos){
            Matcher matcher = SIDE_PATTERN.matcher(field.trim());
            if(matcher.find()){
                return true;
            }
        }

        return false;
    }
}
