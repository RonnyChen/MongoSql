# MongoSql
Parsing SQL to MongoDB
模块名：MongoSqlOpen

====================目的===================
1 直接将部分SQL转换成mongodb的语句访问mongodb
2 简化JAVA驱动访问mongodb的操作，传统的构造doucument的方式还是比较麻烦，另外mongodb也经常升级JAVA驱动，采用模块化的方式便于后期运维
3 适用于那些SQL并不复杂的业务（比如不涉及join,只是简单查询，简单聚合运算）迁移到MongoDB,减少开发成本
4 不必受制于传统sql的思维，比如不需先创建表，查询的字段可以之前没定义
5 如果采用sql的方式操作Mongodb有些场景可能无法适应，比如有时候用内嵌文档可以更好的解决问题，本模块只是一个便捷的工具，具体如何应用需
  要根据业务的实际需求进行规划
6 水平有限，希望大家多提建议，如果有任何疑问欢迎写邮件给我， 陈荣耀-->673571675@qq.com
  
====================功能===================
1 只支持部分SQL,不支持Join和一些复杂SQL！！！
2 目前支持select（部分）、insert、drop，详细信息请看下文
3 使用方法很简单，包com.mongosql.task里面是测试方法，及测试样例：
  ParseSql ps = new ParseSql(dbName, sql);//传入需要查询的数据库名，sql
  ps.sqlToMongo(mongoClient);//传入MongoClient的对象

==================一些注意点==============
1 查询的时候会把对应的字段名和表名全部转成大写，表结构设计的时候需要注意！！
2 select a，sum(a) from t group by a 这种group by 操作会为sum(a)自动生成一个别名，A--SUM，其他聚合函数也是这样,比如A--MAX,A--MIN
3 group by 操作都是用mongodb中aggregate方法实现
4 目前只支持sum,min,max,count函数

以下给出一些样例sql,如果对自己的sql不确定是否支持，试下就知道 

*****************不支持********************
1、select * from class where id not in (select id from class1)
2、select a.id,a.age from people a where a.name='a' and a.age=1  --这种别名的解析目前不支持 
3、不支持order by 2，order by count(a)这种，order by 之后只能跟着字段名 
4、group by 不支持函数，比如group by trim(a)
5、select length(a) from test; 

*****************支持********************
1、select * from class where id between 1 and 5 order by id DESC;
2、select * from class where address is null; 
3、SELECT SESSIONID, SRC_IP FROM NB_TAB_HTTP WHERE clue_id in ('2','5',3);
4、select * from class limit 3;
5、SELECT a,b,c,d,e,f,g FROM test where a=1 and b='hello' and d in
('1',2,'a') and e between 1 and 2 and (f >3 or e<5 or g>=6) and h is
not null and i<>6 order by a DESC,b limit 3;
6、select a,count(*),count(b),sum(b),max(b),min(b) from test where c=1 group by a;
7、select count(*),count(b),sum(b),max(b),min(b) from test where c=1;
8、select distinct a,b from test where c=1;
9、select a from test where c like '1%';  sql中的正则只支持部分

-------
insert into t1(id,name,age,address,phone,sex) values(1,'chen',23,'nanjing',11111,0);

-------
drop table t1;
