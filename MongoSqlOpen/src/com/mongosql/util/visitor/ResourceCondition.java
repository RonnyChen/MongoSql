package com.mongosql.util.visitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongosql.utils.LoggerFactory;

/**
 * 
 * 保存sql解析的结果
 * 
 * @author 陈荣耀
 * @version [v1, 2015年7月4日]
 * @see [ParseSql]
 * @since [MongoSql]
 */
public class ResourceCondition {

	public static enum AggType {
		/**
		 * count statement
		 */
		COUNT,
		/**
		 * sum statement
		 */
		SUM,
		/**
		 * max statement
		 */
		MAX,
		/**
		 * min statement
		 */
		MIN;
	}

	public static enum SqlType {
		/**
		 * insert statement
		 */
		INSERT,
		/**
		 * select statement
		 */
		SELECT,
		/**
		 * drop statement
		 */
		DROP;
	}

	public static final Logger LOG = LoggerFactory
			.getLogger(ResourceCondition.class);

	private String tablename;

	private Set<String> columns = new HashSet<String>();

	private boolean returnAll;

	private boolean countX;

	private boolean isDistinct;

	private int returnRowNumber;

	private Map<AggType, Set<String>> aggColumns = new HashMap<AggType, Set<String>>(); // 聚合字段

	private Set<String> groupcolums = new HashSet<String>();

	private Set<String> wherenotnull = new HashSet<String>();

	private Set<String> whereisnull = new HashSet<String>();

	private List<String> insertcolumns = new ArrayList<String>();

	private List<Object> insertvalues = new ArrayList<Object>();

	// actiontype标记该操作是select、insert、drop还是delete
	private SqlType sqlType = null;

	// 排序字段
	private Document sortCondition = new Document();

	// 其它查询条件
	private Document condition;

	// insert语句的document
	Document insertDocument = null;

	/**
	 * 构造方法 创建资源规则,默认不返回所有字段，返回条数限制为1000
	 * 
	 */
	public ResourceCondition() {
		returnAll = false;
		returnRowNumber = 0;
	}

	/**
	 * Get sql type,such as "select/insert"
	 * 
	 * @return SqlType
	 * @see SqlType
	 */
	public SqlType getSqlType() {
		return sqlType;
	}

	public void setSqlType(SqlType sqlType) {
		this.sqlType = sqlType;
	}

	// ------------------insert ---------------------//
	public void setInsertDocument(Document insertDocument) {
		this.insertDocument = insertDocument;
	}

	public Document getInsertDocument() {
		return insertDocument;
	}

	// ------------------select-----------------------//

	/**
	 * 增加需要非空的WHERE条件
	 * 
	 * @param column
	 *            需要非空的WHERE条件
	 */
	public void addWhereIsNullColumn(String column) {
		whereisnull.add(column);
	}

	/**
	 * 重置需要非空的WHERE条件
	 * 
	 * @param column
	 *            需要非空的WHERE条件
	 */
	public void setWhereIsNullColumn(Set<String> column) {
		whereisnull = column;
	}

	/**
	 * 获得需要设置的需要反馈的列
	 * 
	 * @return 需要非空的WHERE条件
	 */
	public Set<String> getWhereIsNullColumn() {
		return whereisnull;
	}

	/**
	 * 清理需要非空的WHERE条件
	 */
	public void clearWhereNotNullColumn() {
		wherenotnull.clear();
	}

	/**
	 * 增加需要非空的WHERE条件
	 * 
	 * @param column
	 *            需要非空的WHERE条件
	 */
	public void addWhereNotNullColumn(String column) {
		wherenotnull.add(column);
	}

	/**
	 * 重置需要非空的WHERE条件
	 * 
	 * @param column
	 *            需要非空的WHERE条件
	 */
	public void setWhereNotNullColumn(Set<String> column) {
		wherenotnull = column;
	}

	/**
	 * 获得需要设置的需要反馈的列
	 * 
	 * @return 需要非空的WHERE条件
	 */
	public Set<String> getWhereNotNullColumn() {
		return wherenotnull;
	}

	/**
	 * 清理需要主键列
	 */
	public void clearGroupColumn() {
		groupcolums.clear();
	}

	/**
	 * 增加主键列
	 * 
	 * @param column
	 */
	public void addGroupColumn(String column) {
		groupcolums.add(column);
	}

	/**
	 * 重置主键
	 * 
	 * @param column
	 *            主键
	 */
	public void setGroupColumn(Set<String> column) {
		groupcolums = column;
	}

	/**
	 * 获得需要设置的需要反馈的列
	 * 
	 * @return 主键
	 */
	public Set<String> getGroupColumn() {
		return groupcolums;
	}

	/**
	 * 增加需要返回的列
	 * 
	 * @param column
	 *            需要返回的列
	 */
	public void addResourceColumn(String column) {
		columns.add(column);
	}

	/**
	 * 设置需要反馈的列
	 * 
	 * @param columns
	 *            需要反馈的列
	 */
	protected void setResourceColumns(Set<String> columns) {
		this.columns = columns;
	}

	/**
	 * 获得需要设置的需要反馈的列
	 * 
	 * @return 需要反馈的列
	 */
	public Set<String> getResourceColumns() {
		return columns;
	}

	/**
	 * 是否返回该资源表中全部资源列
	 * 
	 * @return true OR false
	 */
	public boolean isReturnAll() {
		return returnAll;
	}

	/**
	 * 设置是否返回该资源表全部资源列
	 * 
	 * @param returnAll
	 *            true OR false
	 */
	public void setReturnAll(boolean returnAll) {
		this.returnAll = returnAll;
	}

	/**
	 * 获得返回最大行数
	 * 
	 * @return 允许返回的最大行数
	 */
	public int getReturnRowNumber() {
		return returnRowNumber;
	}

	/**
	 * 设置改资源每次查询返回的最大行数（默認為1000）
	 * 
	 * @param returnRowNumber
	 *            每次查询返回的最大行数
	 */
	public void setReturnRowNumber(int returnRowNumber) {
		this.returnRowNumber = returnRowNumber;
	}

	/**
	 * 获得查询的表名
	 * 
	 * @return 表名
	 */
	public String getTablename() {
		return tablename;
	}

	/**
	 * 指定查询的表名
	 * 
	 * @return 表名
	 */
	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	/**
	 * 获得排序列表
	 * 
	 * @return
	 */
	public Document getSortCondition() {
		return sortCondition;
	}

	public void addSortCondition(Document sortmap) {
		sortCondition.putAll(sortmap);
	}

	/**
	 * 赋予新的查询条件
	 * 
	 * @param condition
	 */
	public void setCondition(Document condition) {
		this.condition = condition;
	}

	/**
	 * 获得其它查询条件
	 * 
	 * @return
	 */
	public Document getCondition() {
		return condition;
	}

	/**
	 * 返回是否包含distinct
	 * 
	 * @return true OR false
	 */
	public Boolean isDistinct() {
		return isDistinct;
	}

	/**
	 * 设置是否包含distinct
	 * 
	 * @param isDistinct
	 *            true OR false
	 */
	public void setIsDistinct(boolean isDistinct) {
		this.isDistinct = isDistinct;
	}

	/**
	 * 是否包含count(*)
	 * 
	 * @return true OR false
	 */
	public boolean isCountX() {
		return countX;
	}

	/**
	 * 设置是否包含count(*)
	 * 
	 * @param countX
	 *            true OR false
	 */
	public void setCountX(boolean countX) {
		this.countX = countX;
	}

	/**
	 * 返回聚合函数对应聚合字段
	 * 
	 * @return
	 */
	public Map<AggType, Set<String>> getAggColumns() {
		return aggColumns;
	}
	/**
	 * 设置聚合函数对应聚合字段
	 * 
	 * @param aggColumns
	 */
	public void setAggColumns(Map<AggType, Set<String>> aggColumns) {
		this.aggColumns = aggColumns;
	}
	/**
	 * 添加聚合函数对应聚合字段
	 * 
	 * @param aggColumns
	 */
	public void addAggColumns(Map<AggType, Set<String>> aggColumns) {
		this.aggColumns.putAll(aggColumns);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append('\n');
		sb.append("-----ResourceCondition------");
		sb.append('\n');
		sb.append("tablename=" + tablename);
		sb.append('\n');
		sb.append("returnAll=" + returnAll);
		sb.append('\n');
		sb.append("returnRowNumber=" + returnRowNumber);
		sb.append('\n');
		sb.append("sortlist=" + sortCondition);
		sb.append('\n');
		sb.append("columns=" + columns);
		sb.append('\n');
		sb.append("groupcolums=" + groupcolums);
		sb.append('\n');
		sb.append("aggColumns=" + aggColumns);
		sb.append('\n');
		sb.append("wherenotnull=" + wherenotnull);
		sb.append('\n');
		sb.append("whereisnull=" + whereisnull);
		sb.append('\n');
		sb.append("insertcolumns=" + insertcolumns);
		sb.append('\n');
		sb.append("insertvalues=" + insertvalues);
		sb.append('\n');
		sb.append("[");
		if (condition != null) {
			sb.append("condition=" + condition);
			sb.append('\n');
		}
		sb.append("]");
		sb.append('\n');
		sb.append("-----------------------------*");
		return sb.toString();
	}

}
