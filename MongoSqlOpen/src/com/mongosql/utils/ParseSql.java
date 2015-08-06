package com.mongosql.utils;

import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bson.Document;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;

import com.mongosql.util.visitor.ResourceCondition.AggType;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongosql.util.visitor.ResourceCondition;
import com.mongosql.util.visitor.ResourceCondition.SqlType;
import com.mongosql.util.visitor.TraversalVisitor;

/**
 * 
 * sql解析的包装类 包含了简单的测试sql解析方法,提供parse()方法进行sql解析
 * 
 * @author 陈荣耀
 * @version [v1, 2015年7月4日]
 * @see [ResourceCondition]
 * @since [MongoSql]
 */

public class ParseSql {

	private String sql;

	private int reqlimit;

	private String dbName;

	private ResourceCondition resourceCondition;

	public static final Logger LOG = LoggerFactory.getLogger(ParseSql.class);

	public static int count = 0;
	
    // groupby条件
    protected Document groupbyCondition = new Document();
	
    // groupby查询字段
    protected Document projectFields = new Document();
    
    // groupby后展示字段
    protected Document project = new Document();    

	/**
	 * 构造方法
	 * 
	 * @param dbName
	 *            数据库名
	 * @param sql
	 *            需要解析的sql
	 * @param limit
	 *            返回条数
	 */
	public ParseSql(String dbName, String sql) {
		this.dbName = dbName;
		this.sql = sql;
		this.reqlimit = 1000;
		parse();
	}

	public ParseSql(String dbName, String sql, int limit) {
		this.dbName = dbName;
		this.sql = sql;
		this.reqlimit = limit;
		parse();
	}

	/**
	 * 解析sql的主要方法，将sql解析后封装到ResourceCondition中
	 * 
	 * @return ResourceCondition
	 * @throws ParseException
	 * @see ResourceCondition
	 */
	public ResourceCondition parse() {

		Statement statement = null;
		try {

			statement = new CCJSqlParser(new StringReader(sql)).Statement();
			TraversalVisitor tv = new TraversalVisitor();
			statement.accept(tv);
			resourceCondition = tv.getResourceCondition();
			// LOG.info(resourceCondition);

		} catch (ParseException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}

		return resourceCondition;
	}

	/**
	 * 将解析后sql放到mongodb中进行操作
	 * 
	 * @param MongoClient
	 *            mongo连接的对象
	 * @see [类、类#方法、类#成员]
	 */
	public void sqlToMongo(MongoClient cli) {

		SqlType sqlType = resourceCondition.getSqlType();
		count++;
		if (sqlType == SqlType.INSERT) {
			LOG.info("=============insert start==============");
			// 执行insert操作
			insertMongo(cli);
			LOG.info("----insert end---");
		} else if (sqlType == SqlType.SELECT) {
			LOG.info("=============select  start===============");
			// 执行select操作
			Map<String, ArrayList<Document>> result = selectMongo(cli);

			for (String table : result.keySet()) {
				for (Document line : result.get(table))
					System.out.println("LINE--" + count + "--->" + line);
			}
			LOG.info("----select  end---");
		} else if (sqlType == SqlType.DROP) {
			LOG.info("=============drop start==============");
			dropMongoTable(cli);
			LOG.info("----drop  end---");

		}
	}

	/**
	 * 查询mongodb的方法
	 * 
	 * @param req
	 *            查询请求
	 * @param client
	 *            mongodb连接池的对象
	 * @param protocolMap
	 *            字段名映射关系，比如查询海量库需要字段转换
	 */
	public Map<String, ArrayList<Document>> selectMongo(MongoClient client) {
		Map<String, ArrayList<Document>> tmpResult = new HashMap<String, ArrayList<Document>>();
		ArrayList<Document> valueResult = new ArrayList<Document>();

		// 查询的表名
		String tablename = resourceCondition.getTablename();
		// 获得限制返回的条数，默认值为1000
		int limit = resourceCondition.getReturnRowNumber();
		if (limit == 0)
			limit = reqlimit;
		// 获得查询条件
		Document condition = resourceCondition.getCondition();
		// 获得查询字段
		Document findFields = new Document();
		for (String col : resourceCondition.getResourceColumns()) {
			findFields.append(col, true);
		}
		// 获得排序字段
		Document sortlist = resourceCondition.getSortCondition();

		// 打印日志
		LOG.info("tablename:" + tablename);
		LOG.info("dbname:" + dbName);
		LOG.info("condtion:" + condition);
		LOG.info("findfields:" + findFields);
		LOG.info("limit:" + limit);

		// 连接到mongodb进行查询
		setProject(resourceCondition.getCondition());
		if (resourceCondition.isDistinct()) {
			// distinct sql
		    findDistinct(client, valueResult,  condition, sortlist, tablename);
		} else if (resourceCondition.getGroupColumn().isEmpty()
				&& resourceCondition.getAggColumns().isEmpty()) {
			// 普通查询
			find(client, valueResult, tablename, limit, condition, findFields,
					sortlist);
		} else if (resourceCondition.getGroupColumn().isEmpty() && !(resourceCondition.getAggColumns().isEmpty())){
		    findGroupby(client, valueResult, condition, sortlist, tablename);
		} else findGroupby(client, valueResult, condition, sortlist, tablename);
		tmpResult.put(tablename, valueResult);
		return tmpResult;
	}

	/**
	 * mongodb find方法
	 * @param client
	 * @param valueResult
	 * @param tablename
	 * @param limit
	 * @param condition
	 * @param findFields
	 * @param sortlist
	 */
	private void find(MongoClient client, ArrayList<Document> valueResult,
			String tablename, int limit, Document condition,
			Document findFields, Document sortlist) {
		FindIterable<Document> cur = null;
		if (sortlist.isEmpty())
			cur = client.getDatabase(dbName).getCollection(tablename)
					.find(condition).projection(findFields).limit(limit);
		else {
			cur = client.getDatabase(dbName).getCollection(tablename)
					.find(condition).projection(findFields).sort(sortlist)
					.limit(limit);
			LOG.info("sortlist:" + sortlist);
		}
		for (Document row : cur)
			valueResult.add(row);
	}

	/**
	 * mongodb的insert方法，目前为支持单条sql insert
	 * 
	 * @param client
	 *            mongodb连接池的对象
	 * 
	 */
	public Map<String, Map<String, Object>> insertMongo(MongoClient client) {
		Map<String, Map<String, Object>> tmpResult = new HashMap<String, Map<String, Object>>();
		// 查询的表名
		String tablename = resourceCondition.getTablename();
		// 获得insert的document
		Document insertDocument = resourceCondition.getInsertDocument();

		if (insertDocument != null) {

			// 连接mongodb进行查询
			client.getDatabase(dbName).getCollection(tablename)
					.insertOne(insertDocument);
			Map<String, Object> tmpMap = new HashMap<String, Object>();
			tmpMap.put("insert", "succes");
			tmpResult.put(tablename, tmpMap);

			// 打印日志
			LOG.info("Success:-----insert success------");
			LOG.info("tablename:" + tablename);
			LOG.info("dbname:" + dbName);
			LOG.info("Document:" + insertDocument);
			LOG.info("--------------------");
		} else {
			LOG.info("Error: [insert] the number of columns is not equle number of values!");
		}

		return tmpResult;
	}

	/**
	 * 删除mongodb中的表 <功能详细描述>
	 * 
	 * @param client
	 * @return
	 * @see [类、类#方法、类#成员]
	 */
	public Map<String, String> dropMongoTable(MongoClient client) {
		Map<String, String> statusMap = new HashMap<String, String>();
		// 查询的表名
		String tablename = resourceCondition.getTablename();
		// 执行删除
		try {
			client.getDatabase(dbName).getCollection(tablename).drop();
		} catch (Exception e) {
			statusMap.put(tablename, "failed droped");
			LOG.error(e.getMessage());
		}
		statusMap.put(tablename, "success droped");

		return statusMap;
	}
	/**
	 * mongodb aggregate方法，用于实现distinct
	 * @param client
	 * @param valueResult
	 * @param condition
	 * @param sortlist
	 * @param tn
	 */
    private void findDistinct(MongoClient client, ArrayList<Document> valueResult, Document condition,
        Document sortlist, String tn) {
        setDistinctCondition(resourceCondition);
        AggregateIterable<Document> iterable;
        if (sortlist.isEmpty()) {
            iterable =
                client.getDatabase(dbName)
                    .getCollection(tn)
                    .aggregate(Arrays.asList(new Document("$match", condition),
                        new Document("$project", projectFields),
                        new Document("$group", groupbyCondition),
                        new Document("$project", project)));
        }
        else {
            iterable =
                client.getDatabase(dbName)
                    .getCollection(tn)
                    .aggregate(Arrays.asList(new Document("$match", condition),
                        new Document("$project", projectFields),
                        new Document("$group", groupbyCondition),
                        new Document("$project", project),
                        new Document("$sort", sortlist)));
        }
        for (Document row : iterable) {
            valueResult.add(row);
        }
    }
    /**
     * 解析distinct字段
     * 
     */
    public void setDistinctCondition(ResourceCondition rc) {
        if (rc.isDistinct()) {
            Document group = new Document();                        //分组字段
            projectFields.append("_id", 0);                         //不查询_id
            project.append("_id", 0);                               //不查询分组后的_id
            for (String col : rc.getResourceColumns()) {            //通过group by 实现distinct
                String strUpper=col.toUpperCase();
                group.append(strUpper, "$" + strUpper);             //获得分组字段
                projectFields.append(strUpper, "$" + strUpper);     //只查询需要字段
                project.append(strUpper, "$_id." + strUpper);       //映射分组查询后的_id
            }
            groupbyCondition.append("_id", group);                  //分组字段传递给_id
        }
    }
    /**
     * mongodb aggregate方法，实现group by
     * @param client
     * @param valueResult
     * @param condition
     * @param sortlist
     * @param tn
     */
    private void findGroupby(MongoClient client, ArrayList<Document> valueResult, Document condition,
        Document sortlist, String tn) {
        AggregateIterable<Document> iterable;
        setGroupbyCondition(resourceCondition);
        if (sortlist.isEmpty()) {
            iterable =
                client.getDatabase(dbName)
                    .getCollection(tn)
                    .aggregate(Arrays.asList(new Document("$match", condition),
                        new Document("$project", projectFields),
                        new Document("$group", groupbyCondition),
                        new Document("$project", project)));
        }
        else {
            iterable =
                client.getDatabase(dbName)
                    .getCollection(tn)
                    .aggregate(Arrays.asList(new Document("$match", condition),
                        new Document("$project", projectFields),
                        new Document("$group", groupbyCondition),
                        new Document("$project", project),
                        new Document("$sort", sortlist)));
        }
        for (Document row : iterable) {
            valueResult.add(row);
        }
    }
    /**
     * 解析group by条件
     * @param rc
     */
    public void setGroupbyCondition(ResourceCondition rc) {
        Document group = new Document();                    //分组字段
        for (String col : rc.getGroupColumn()) {            //获得分组字段
            String strUpper=col.toUpperCase();
            group.append(strUpper, "$" + strUpper);         //获得分组字段
            projectFields.append(strUpper, 1);              //只查询需要字段
            project.append(strUpper, "$_id." + strUpper);   //映射分组查询后的_id
        }
        groupbyCondition.append("_id", group);                        //分组字段传递给_id
        if (rc.isCountX()) {                                          //通过sum实现count(*)
            projectFields.append("_id", 1);                           //查询_id
            groupbyCondition.append("COUNT--ALL", new Document("$sum", 1));//所有_id赋值为1并进行sum实现count(*)
            project.append("COUNT--ALL", "$COUNT--ALL");                        //映射sum的结果字段
        }
        else
            projectFields.append("_id", 0);                           //不查询_id
        project.append("_id", 0);                                     //不展现group by后的_id字段
        Map<AggType, Set<String>> aggColumns = rc.getAggColumns();                                    //聚合函数及对应字段
        Set<AggType> aggKeys=aggColumns.keySet();
        for (AggType aggKey : aggKeys) {
            Set<String> aggCols = aggColumns.get(aggKey);                                             //聚合字段
            String aggKeyString=aggKey.toString();
            String aggKeyLower=aggKey.toString().toLowerCase();
            if (aggKey == AggType.COUNT) {
                for (String col : aggCols) {
                    List<String> cmplist = Arrays.asList("$" + col, null);                            //[$col,null]
                    Document cmp = new Document("$cmp", cmplist);                                     //$cmp:[$col,null]
                    List<Serializable> cmplist2 = Arrays.asList(cmp, -1);                             //[{$cmp:[$col,null]},-1]
                    Document cmp2 = new Document("$cmp", cmplist2);                                   //$cmp:[{$cmp:[$col,null]},-1]
                    projectFields.append(col + "--cmp", cmp2);                                         //{col+"--cmp":{$cmp:[{$cmp:[$col,null]},-1]}}
                    groupbyCondition.append(col + "--COUNT", new Document("$sum", "$" + col + "--cmp"));//{col+"--COUNT":{$sum:$col+"--cmp"}}
                    project.append(col + "--COUNT", 1);
                }
            }
            else {
                for (String col : aggCols) {
                    projectFields.append(col, 1);                                                                   //映射查询返回字段
                    groupbyCondition.append(col + "--" + aggKeyString, new Document("$" + aggKeyLower, "$" + col));  //聚合操作
                    project.append(col + "--" + aggKeyString, 1);                                                    //展示结果
                }
            }
        }
    }
    
    /**
     * 
     * @param document
     */
    @SuppressWarnings("unchecked")
    private void setProject(Document document) {
        for (String str : document.keySet()) {
            if (!(str.equals("$or")))
                projectFields.append(str, 1);
            else
                parserOrCondition((ArrayList<Document>)document.get(str));
        }
    }
    
    /**
     * 
     * @param docuList
     */
    private void parserOrCondition(ArrayList<Document> docuList){
        for (Document document : docuList) {
            setProject(document);
        }
    }
}
