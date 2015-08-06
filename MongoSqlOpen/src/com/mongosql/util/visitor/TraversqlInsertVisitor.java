package com.mongosql.util.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongosql.util.visitor.ResourceCondition.SqlType;
import com.mongosql.utils.CommonUtil;
import com.mongosql.utils.LoggerFactory;

import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.IntoTableVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * 
 * 用于解析insert语句,目前不支持多媒体文件insert
 * insert 默认会把所有字段名和表名全部转成大写！！
 * 
 * @author  凌翔
 * @version  [V1, 2015年7月9日]
 * @see  [TraversalVisitor]
 * @since  [MongoSql]
 */
public class TraversqlInsertVisitor implements IntoTableVisitor, ItemsListVisitor {
    /**
     * 日志对象
     */
    public static final Logger LOG = LoggerFactory.getLogger(TraversqlInsertVisitor.class);
    
    private ResourceCondition resourceCondition;
    
    private Insert insert;
    
    private List<String> columnList = new ArrayList<String>();
    
    private List<Object> valuesList = new ArrayList<Object>();
    
    private Document insertDocument=new Document();
    
    /**
     * <默认构造函数>
     */
    public TraversqlInsertVisitor(ResourceCondition resourceCondition, Insert insert) {
        this.resourceCondition = resourceCondition;
        this.insert = insert;
        init();
        
    }
    
    public void init() {
        
        //设置类型为INSERT语句
        resourceCondition.setSqlType(SqlType.INSERT);
        //设置表名
        resourceCondition.setTablename(insert.getTable().getName().toUpperCase());
        
        if (insert.isUseValues()) {
            for (Object i : insert.getColumns()) {
                columnList.add(i.toString().toUpperCase());
            }
        }
        
        ItemsList itemlist = insert.getItemsList();
        itemlist.accept(this);
        
        if (columnList.size() == valuesList.size()) {
            for (int i = 0; i < columnList.size(); i++)
                insertDocument.put(columnList.get(i), valuesList.get(i));
            //把insert的document赋值给resourceCondition
            resourceCondition.setInsertDocument(insertDocument);
        }
        else {
            LOG.error("insert statement error!!");
        }
        
    }
    
    @Override
    public void visit(ExpressionList expressionList) {
        LOG.trace("visit expressionList:" + expressionList.toString());
        
        for (int i = 0; i < expressionList.getExpressions().size(); i++) {
            LOG.trace(expressionList.getExpressions().get(i).toString());
            valuesList.add(CommonUtil.ObjectTypeConvert(expressionList.getExpressions().get(i)));
        }
        
    }
    
    @Override
    public void visit(Table table) {
        // TODO Auto-generated method stub
    }
    
    @Override
    public void visit(SubSelect arg0) {
        // TODO Auto-generated method stub
        
    }
    
    /* 自定义方法分割线 */
    
    public ResourceCondition getResourceCondition() {
        return resourceCondition;
    }
    
}
