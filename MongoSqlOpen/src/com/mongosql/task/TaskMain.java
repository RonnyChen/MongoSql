package com.mongosql.task;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.ServerAddress;
import com.mongosql.utils.LoggerFactory;
import com.mongosql.utils.ParseSql;

/**
 * 测试程序的主入口
 * 
 * @author  陈荣耀
 * @version  [v1, 2015年8月5日]
 * @see  [ParseSql]
 * @since  [MongoSql]<br>
 * 
 *Not support:<br>
 *1、select * from class where id not in (select id from class1)<br>
 *2、select a.id,a.age from people a where a.name='a' and a.age=1  这种别名的解析目前不支持<br>
 *3、不支持order by 2，order by count(a)这种，order by 之后只能跟着字段名 <br>
 *4、group by 不支持函数，比如group by trim(a)<br>
 *5、目前只支持sum,min,max,count函数
 *
 *Support:<br>
 *1、select * from class where id between 1 and 5 order by id DESC;<br>
 *2、select * from class where address is null; <br>
 *3、SELECT ID, IP FROM t1 WHERE id in ('2','5',3);<br>
 *4、select * from class limit 3;<br>
 *5、SELECT a,b,c,d,e,f,g FROM test where a=1 and b='hello' and d in
 ('1',2,'a') and e between 1 and 2 and (f >3 or e<5 or g>=6) and h is
 not null and i<>6 order by a DESC,b limit 3;<br>
 *6、select a,count(*),count(b),sum(b),max(b),min(b) from test where c=1 group by a;<br>
 *7、select count(*),count(b),sum(b),max(b),min(b) from test where c=1;
 *8、select distinct a,b from test where c=1;<br>
 *9、select a from test where c like '1%';  sql中的正则只支持部分<br>
 * 
 */
public class TaskMain {
    
    /**
     * 配置文件路径
     */
    private final static String configPath = "conf" + File.separator + "MongoConf.xml";
    
    // mongodb多实例的连接池,需要读配置文件中的IP和端口把实例加到连接池中
    private final static List<MongoClient> connPool = new ArrayList<MongoClient>();
    
    public static final Logger LOG = LoggerFactory.getLogger(TaskMain.class);
    
    /**
     * <默认构造函数>
     */
    TaskMain() {
        createConnect(configPath);
    }
    
    /**
     * 读取配置文件，创建连接池
     * 
     * @param path
     */
    public void createConnect(String path) {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(path);
            // 读取服务器配置信息
            NodeList hostconfig = doc.getElementsByTagName("HOST");
            Map<ServerAddress, Map<String, Object>> confMap = new HashMap<ServerAddress, Map<String, Object>>();
            Map<String, Object> conf = null;
            // 默认线程池大小
            int poolSize = 100;
            String address = null;
            for (int i = 0; i < hostconfig.getLength(); i++) {
                Node field = hostconfig.item(i);
                conf = new HashMap<String, Object>();
                // 每个host中的小循环
                for (Node node = field.getFirstChild(); node != null; node = node.getNextSibling()) {
                    if (node.getNodeType() == Node.ELEMENT_NODE) {
                        if (node.getNodeName().equals("POOLSIZE")) {
                            poolSize = Integer.valueOf(node.getFirstChild().getNodeValue());
                            conf.put("POOLSIZE", poolSize);
                        }
                        else if (node.getNodeName().equals("IPLIST")) {
                            address = node.getFirstChild().getNodeValue();
                        }
                    }
                }
                confMap.put(new ServerAddress(address), conf);
            }
            LOG.info("confMap-->" + confMap);
            // 创建mongodb的连接串，将连接放到connPool中
            for (Iterator<ServerAddress> it = confMap.keySet().iterator(); it.hasNext();) {
                Builder builder = new Builder();
                ServerAddress sa = it.next();
                builder.connectionsPerHost((Integer)confMap.get(sa).get("POOLSIZE"));
                connPool.add(new MongoClient(sa, builder.build()));
            }
        }
        catch (Exception e) {
            LOG.error("ERROR!! Cannot create Mongodb's connection! error message-->" + e.getMessage());
        }
    }
    
    public static void main(String args[]) {
        
        new TaskMain();
        String dbName = "test";
        
        //测试样例：
        //先插入数据
        List<String> insertSql = new ArrayList<String>();
        insertSql.add("insert into t1(id,name,age,address,phone,sex) values(1,'chen',23,'nanjing',11111,0)");
        insertSql.add("insert into t1(id,name,age,address,phone,sex) values(2,'zhang',20,'nanjing',22222,0)");
        insertSql.add("insert into t1(id,name,age,address,phone,sex) values(3,'zhang',24,'beijing',33333,1)");
        insertSql.add("insert into t1(id,name,age,address,phone,sex) values(4,'zhang',40,'shanghai',2147483647000,0)");
        //自动解析
        for (String i : insertSql) {
            ParseSql ps = new ParseSql(dbName, i);
            for (MongoClient cli : connPool) {
                ps.sqlToMongo(cli);
            }
        }
        
        //各种查询语句
        List<String> selectSql = new ArrayList<String>();
        selectSql.add("select * from t1 where id='02'");
        selectSql.add("select * from t1 where id in ('02',1)");
        selectSql.add("select * from t1 where (id in ('02',1)  or sex=0)");
        selectSql.add("select * from t1 where age >10.1 and age <30");
        selectSql.add("select * from t1 where name='zhang' " + "and age between 20 and 30 "
            + "and address in ('nanjing','beijing') and  ( phone=22222 or phone=66666 or phone=99999 ) and sex>=0  "
            + " order by id desc,name  limit 10");
        selectSql.add("select * from t1 where age is not  null");
        selectSql.add("select * from t1 where id is null or age is not null");
        selectSql.add("select * from t1 where id is null ");
        
        selectSql.add("select * from t1 where name<>'chen'");
        selectSql.add("select * from t1 order by name desc limit 3");
        selectSql.add("select * from t1 where name like 'zh%' ");
        selectSql.add("select distinct age from t1 where name like 'zh%' ");
        selectSql.add("select count(*),count(age),sum(age),max(age),min(age) from t1");
        selectSql.add("select count(*),count(age),sum(age),max(age),min(age) from t1 where name like 'zh%' ");
        selectSql.add("select name,count(*),count(age),sum(age),max(age),min(age) from t1 where name like 'zh%' group by name");
        selectSql.add("select count(*),count(age),sum(age),max(age),min(age) from t1 where name='zhang' and (age=20 or ADDRESS='beijing')");
        selectSql.add("select count(*),count(age),sum(age),max(age),min(age) from t1 where name like 'z%'");
        selectSql.add("select name,count(*),count(age),sum(age),max(age),min(age) from t1 where ADDRESS='nanjing' and name='chen' group by name");
        selectSql.add("select name,count(*),count(age),sum(age),max(age),min(age) from t1 group by name");
        selectSql.add("select name,count(*),count(age),sum(age),max(age),min(age) from t1 where name='zhang' and (age=20 or ADDRESS='beijing') group by name ");
        selectSql.add("select distinct age from t1 where name like 'z%' and age=20 ");
       
        //删除表
        selectSql.add("drop table t1");
        
        for (String i : selectSql) {
            ParseSql ps = new ParseSql(dbName, i);
            ps.sqlToMongo(connPool.get(0));
        }
        
    }
    
}
