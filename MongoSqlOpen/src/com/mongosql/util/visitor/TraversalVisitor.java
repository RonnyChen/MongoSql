package com.mongosql.util.visitor;

import org.apache.log4j.Logger;

import com.mongosql.utils.LoggerFactory;

import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

/**
 * 
 * 用于解析sql,会判断sql究竟是何种DML操作，并根据各自的类型选用各自的解析方法
 * 目前只支持select语句
 * 
 * @author  陈荣耀
 * @version  [v1, 2015年7月4日]
 * @see  [ParseSql]
 * @since  [MongoSql]
 */

public class TraversalVisitor implements StatementVisitor {
    /**
     * 日志对象
     */
    
    public static final Logger LOG = LoggerFactory.getLogger(TraversalVisitor.class);
    
    private TraversqlSelectVisitor selectv;
    
    private TraversqlInsertVisitor insertv;
    
    private TraversqlDropVisitor dropv;
    
    private ResourceCondition resourceCondition;
    
    @Override
    public void visit(Select select) {
        selectv = new TraversqlSelectVisitor(new ResourceCondition());
        select.getSelectBody().accept(selectv);
    }
    
    @Override
    public void visit(Insert insert) {
        insertv = new TraversqlInsertVisitor(new ResourceCondition(), insert);
    }
    
    @Override
    public void visit(Drop drop) {
        dropv = new TraversqlDropVisitor(new ResourceCondition(), drop);
        
    }
    
    @Override
    public void visit(Delete arg0) {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void visit(Update arg0) {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void visit(Replace arg0) {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void visit(Truncate arg0) {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void visit(CreateTable arg0) {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * 返回查询对象
     * 
     * @return
     */
    public ResourceCondition getResourceCondition() {
        if (selectv != null) {
            resourceCondition = selectv.getResourceCondition();
        }
        else if (insertv != null) {
            resourceCondition = insertv.getResourceCondition();
        }
        else if (dropv != null) {
            resourceCondition = dropv.getResourceCondition();
        }
        return resourceCondition;
        
    }
}
