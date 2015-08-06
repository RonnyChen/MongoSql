package com.mongosql.util.visitor;

import org.apache.log4j.Logger;

import com.mongosql.util.visitor.ResourceCondition.SqlType;
import com.mongosql.utils.LoggerFactory;

import net.sf.jsqlparser.statement.drop.Drop;

/**
 * 
 * 用于解析drop语句
 * 
 * @author  凌翔
 * @version  [V1, 2015年7月24日]
 * @see  [TraversalVisitor]
 * @since  [MongoSql]
 */
public class TraversqlDropVisitor {
    /**
     * 日志对象
     */
    public static final Logger LOG = LoggerFactory.getLogger(TraversqlDropVisitor.class);
    
    private ResourceCondition resourceCondition;
    
    public TraversqlDropVisitor(ResourceCondition resourceCondition, Drop drop) {
        resourceCondition.setSqlType(SqlType.DROP);
        this.resourceCondition = resourceCondition;
        resourceCondition.setTablename(drop.getName().toUpperCase());
    }
    
    /* 自定义方法分割线 */
    
    public ResourceCondition getResourceCondition() {
        return resourceCondition;
    }
    
}
