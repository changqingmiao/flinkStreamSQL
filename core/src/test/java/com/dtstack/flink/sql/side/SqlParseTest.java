package com.dtstack.flink.sql.side;

import com.dtstack.flink.sql.Main;
import org.apache.flink.calcite.shaded.com.google.common.base.Charsets;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.junit.Test;

import java.net.URLEncoder;
import java.util.List;

/**
 * Created by miaochangqing1 on 2019/2/28.
 */
public class SqlParseTest {

    @Test
    public void testSqlParse() throws Exception {
        String sql = "CREATE TABLE MyTable(\n" +
                "    name varchar,\n" +
                "    channel varchar,\n" +
                "    pv int,\n" +
                "    xctime bigint,\n" +
                "    CHARACTER_LENGTH(channel) AS timeLeng \n" +
                " )WITH(\n" +
                "    type ='kafka09',\n" +
                "    bootstrapServers ='172.16.8.198:9092',\n" +
                "    zookeeperQuorum ='172.16.8.198:2181/kafka',\n" +
                "    offsetReset ='latest',\n" +
                "    topic ='nbTest1',\n" +
                "    parallelism ='1'\n" +
                " );\n" +
                "\n" +
                "CREATE TABLE MyResult(\n" +
                "    channel varchar,\n" +
                "    pv varchar\n" +
                " )WITH(\n" +
                "    type ='mysql',\n" +
                "    url ='jdbc:mysql://172.16.8.104:3306/test?charset=utf8',\n" +
                "    userName ='dtstack',\n" +
                "    password ='abc123',\n" +
                "    tableName ='pv2',\n" +
                "    parallelism ='1'\n" +
                " );\n" +
                "\n" +
                "CREATE TABLE workerinfo(\n" +
                "    cast(logtime as TIMESTAMP) AS rtime,\n" +
                "    cast(logtime) AS rtime\n" +
                " )WITH(\n" +
                "    type ='hbase',\n" +
                "    zookeeperQuorum ='rdos1:2181',\n" +
                "    tableName ='workerinfo',\n" +
                "    rowKey ='ce,de',\n" +
                "    parallelism ='1',\n" +
                "    zookeeperParent ='/hbase'\n" +
                " );\n" +
                "\n" +
                "CREATE TABLE sideTable(\n" +
                "    cf:name varchar as name,\n" +
                "    cf:info varchar as info,\n" +
                "    PRIMARY KEY(name),\n" +
                "    PERIOD FOR SYSTEM_TIME \n" +
                " )WITH(\n" +
                "    type ='hbase',\n" +
                "    zookeeperQuorum ='rdos1:2181',\n" +
                "    zookeeperParent ='/hbase',\n" +
                "    tableName ='workerinfo',\n" +
                "    cache ='LRU',\n" +
                "    cacheSize ='10000',\n" +
                "    cacheTTLMs ='60000',\n" +
                "    parallelism ='1'\n" +
                " );\n" +
                "\n" +
                "insert\n" +
                "into\n" +
                "    MyResult\n" +
                "    select\n" +
                "        d.channel,\n" +
                "        d.info\n" +
                "    from\n" +
                "        (      select\n" +
                "            a.*,b.info\n" +
                "        from\n" +
                "            MyTable a\n" +
                "        join\n" +
                "            sideTable b\n" +
                "                on a.channel=b.name\n" +
                "        where\n" +
                "            a.channel = 'xc2'\n" +
                "            and a.pv=10      ) as d";

        test(sql);
    }

    public void test(String sql) throws Exception {
        List<String> paramList = Lists.newArrayList();
        paramList.add("-sql");
        String exeSql = URLEncoder.encode(sql, Charsets.UTF_8.name());
        paramList.add(exeSql);
        paramList.add("-name");
        paramList.add("xc");
        paramList.add("-localSqlPluginPath");
        paramList.add("E:\\01-myCode\\flinkStreamSQL\\plugins");
        paramList.add("-mode");
        paramList.add("local");
        paramList.add("-addjar");
        paramList.add(URLEncoder.encode("[\"D:\\\\soucecode\\\\rdos-execution-engine\\\\..\\\\tmp140\\\\flink14Test-1.0-SNAPSHOT.jar\"]", Charsets.UTF_8.name()));
        paramList.add("-remoteSqlPluginPath");
        paramList.add("/opt/dtstack/flinkplugin");
        paramList.add("-confProp");
        String conf = "{\"time.characteristic\":\"EventTime\",\"sql.checkpoint.interval\":10000}";
        String confEncode = URLEncoder.encode(conf, Charsets.UTF_8.name());
        paramList.add(confEncode);

        String[] params = new String[paramList.size()];
        paramList.toArray(params);
        Main.main(params);
    }

}
