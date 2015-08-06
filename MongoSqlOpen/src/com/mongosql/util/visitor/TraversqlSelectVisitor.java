package com.mongosql.util.visitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongosql.util.visitor.ResourceCondition.AggType;
import com.mongosql.util.visitor.ResourceCondition.SqlType;
import com.mongosql.utils.CommonUtil;
import com.mongosql.utils.LoggerFactory;

/**
 * 
 * 用于解析select语句
 * 
 * @author  陈荣耀
 * @version  [V1, 2015年7月4日]
 * @see  [TraversalVisitor]
 * @since  [MongoSql]
 */
public class TraversqlSelectVisitor implements SelectVisitor, FromItemVisitor, ExpressionVisitor, OrderByVisitor,
    SelectItemVisitor, ItemsListVisitor {
    /**
     * 日志对象
     */
    public static final Logger LOG = LoggerFactory.getLogger(TraversqlSelectVisitor.class);
    
    private ResourceCondition resourceCondition;
    
    private Document condition=new Document();
    
    private Document nowcondition;
    
    private List<Object> initem = new ArrayList<Object>();
    
    //状态符号，0表示sql只有一个查询条件，对于带有and,or的sql语句需要将它设置成1
    private int status=0;
    
    /**
     * <默认构造函数>
     */
    public TraversqlSelectVisitor(ResourceCondition resourceCondition) {
        resourceCondition.setSqlType(SqlType.SELECT);
        this.resourceCondition = resourceCondition;
    }
    
    public ResourceCondition getResourceCondition() {
        resourceCondition.setCondition(condition);
        return resourceCondition;
    }
    
    public Document getCondition() {
        return condition;
    }
    
    @Override
    public void visit(PlainSelect plainSelect) {
        LOG.trace("visit plainSelect:");
        // select item
        if (null !=plainSelect.getDistinct()) {
            resourceCondition.setIsDistinct(true);
        }
        List<?> selectItems = plainSelect.getSelectItems();
        for (Object item : selectItems) {
            ((SelectItem)item).accept(this);
        }
        
        // from
        plainSelect.getFromItem().accept(this);
        
        // where
        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(this);
        }
        // order by
        List<?> oelements = plainSelect.getOrderByElements();
        if (oelements != null) {
            for (Object e : oelements) {
                ((OrderByElement)e).accept(this);
            }
        }
        // group by
        List<?> gelements = plainSelect.getGroupByColumnReferences();
        if (null != gelements) {
            for (int i = 0; i < gelements.size(); i++) {
                resourceCondition.addGroupColumn(gelements.get(i).toString());
            }
        }
        
        // limit
        if (plainSelect.getLimit() != null) {
            resourceCondition.setReturnRowNumber(new Long(plainSelect.getLimit().getRowCount()).intValue());
        }
        
    }
    
    @Override
    public void visit(Union union) {
        LOG.trace("visit union:");
        
    }
    
    /* form分割线 */
    
    @Override
    public void visit(Table table) {
        LOG.trace("visit tableName:" + table.getName());
        resourceCondition.setTablename(table.getName().toUpperCase());
    }
    
    @Override
    public void visit(SubSelect subselect) {
        LOG.trace("visit subselect:");
        
    }
    
    @Override
    public void visit(SubJoin subjoin) {
        LOG.trace("visit subjoin:");
        
    }
    
    /* where条件分割线 */
    


    @Override
    public void visit(AndExpression andExpression) {
        
        LOG.trace("visit andExpression:" + andExpression.toString());
        status++;
        LOG.trace("------>" + andExpression);
        andExpression.getLeftExpression().accept(this);
        condition.putAll(nowcondition);
        LOG.trace("left------>" + nowcondition);
        andExpression.getRightExpression().accept(this);
        condition.putAll(nowcondition);
        LOG.trace("right----->" + nowcondition);
    }
    
    @Override
    public void visit(OrExpression orExpression) {
       
        int nowStatus=status++;
        LOG.trace("visit orExpression:" + orExpression.toString());
        List<Document> orCondition = new ArrayList<Document>();
        orExpression.getLeftExpression().accept(this);
        orCondition.add(nowcondition);
        orExpression.getRightExpression().accept(this);
        orCondition.add(nowcondition);
        nowcondition = new Document("$or", orCondition);

        if (nowStatus == 0)
            condition = nowcondition;

    }
    
    @Override
    public void visit(InExpression inExpression) {
        LOG.trace("visit inExpression:" + inExpression.toString());
        ItemsList itemlist = inExpression.getItemsList();
        itemlist.accept(this);
        String column = inExpression.getLeftExpression().toString().toUpperCase();
        if (initem.size() > 0) {
            nowcondition = new Document(column, new Document("$in", initem));
            if (status == 0) {
                condition = nowcondition;
            }
        }
    }
    
    @Override
    public void visit(Between between) {
        LOG.trace("visit between:" + between.toString());
        
        String column = between.getLeftExpression().toString().toUpperCase();
        Object start = CommonUtil.ObjectTypeConvert(between.getBetweenExpressionStart());
        Object end = CommonUtil.ObjectTypeConvert(between.getBetweenExpressionEnd());
        nowcondition = new Document(column, new Document("$gte", start).append("$lte", end));
        if (status == 0) {
            condition = nowcondition;
        }
    }
    
    @Override
    public void visit(EqualsTo equalsTo) {
        LOG.trace("visit equalsTo:" + equalsTo.toString());
        //  equalsTo.getRightExpression().accept(this);
        Object value = CommonUtil.ObjectTypeConvert(equalsTo.getRightExpression());
        String column = equalsTo.getLeftExpression().toString().toUpperCase();
        nowcondition = new Document(column, value);
        if (status == 0) {
            condition = nowcondition;
        }
        
    }
    
    @Override
    public void visit(GreaterThan greaterThan) {
        LOG.trace("visit greaterThan:" + greaterThan.toString());
        String column = greaterThan.getLeftExpression().toString().toUpperCase().trim();
        nowcondition = rangeCombine(condition, "$gt", column, greaterThan.getRightExpression());
        if (status == 0) {
            condition = nowcondition;
        }
        
    }
    
    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        LOG.trace("visit greaterThanEquals:" + greaterThanEquals.toString());
        String column = greaterThanEquals.getLeftExpression().toString().toUpperCase().trim();
        nowcondition = rangeCombine(condition, "$gte", column, greaterThanEquals.getRightExpression());
        if (status == 0) {
            condition = nowcondition;
        }
        
    }
    
    @Override
    public void visit(MinorThan minorThan) {
        LOG.trace("visit minorThan:" + minorThan.toString());
        String column = minorThan.getLeftExpression().toString().toUpperCase().trim();
        nowcondition = rangeCombine(condition, "$lt", column, minorThan.getRightExpression());
        if (status == 0) {
            condition = nowcondition;
        }
        
    }
    
    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        LOG.trace("visit minorThanEquals:");
        String column = minorThanEquals.getLeftExpression().toString().toUpperCase().trim();
        nowcondition = rangeCombine(condition, "$lte", column, minorThanEquals.getRightExpression());
        if (status == 0) {
            condition = nowcondition;
        }
        
    }
    
    
    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        LOG.trace("visit notEqualsTo:");
        Object value = CommonUtil.ObjectTypeConvert(notEqualsTo.getRightExpression());
        String column = notEqualsTo.getLeftExpression().toString().toUpperCase();
        nowcondition = new Document(column, new Document("$ne", value));
        if (status == 0) {
            condition = nowcondition;
        }
        
    }
    
    @Override
    public void visit(IsNullExpression isNullExpression) {
        LOG.trace("visit isNullExpression:" + isNullExpression);
        if (isNullExpression.isNot())
            nowcondition =
                new Document(isNullExpression.getLeftExpression().toString().toUpperCase(),
                    new Document("$ne", null).append("$exists", true));
        else
            nowcondition = new Document(isNullExpression.getLeftExpression().toString().toUpperCase(), null);
        
        if (status == 0) {
            condition = nowcondition;
        }
        

    }
    
    @Override
    //TODO
    public void visit(LikeExpression likeExpression) {
        LOG.trace("visit likeExpression:" + likeExpression.toString());
        //需要去除首尾的单引号，并且将%替换成*
        String value = CommonUtil.prString(likeExpression.getRightExpression().toString().trim()).replace("_",".").replace("%", ".*");
        String column = likeExpression.getLeftExpression().toString().toUpperCase();
        Pattern pattern=Pattern.compile(value,Pattern.CASE_INSENSITIVE);
        nowcondition = new Document(column,pattern);
        if (status == 0) {
            condition = nowcondition;
        }
    }
    
  
    @Override
    public void visit(NullValue nullValue) {
        LOG.trace("visit nullValue:");
        
    }
    
    @Override
    public void visit(Function function) {
        Map<AggType, Set<String>> aggColumns = new HashMap<AggType, Set<String>>();                         //聚合字段
        Set<String> col = new HashSet<String>();
        String type = function.getName().toUpperCase();                                                     //聚合函数名
        String[] typeList = {"SUM", "MIN", "MAX", "COUNT"};
        ExpressionList status = function.getParameters();
        if (status != null && function.isAllColumns() == false) {
            String parameter = function.getParameters().getExpressions().get(0).toString().toUpperCase();
            for (String string : typeList) {
                if (type.equals(string)) {
                    col.add(parameter);
                    aggColumns.put(AggType.valueOf(type), col);
                }
            }
            resourceCondition.addAggColumns(aggColumns);
        }
        else {
            resourceCondition.setCountX(true);
        }
        //移除ResourceColumns里作为整体保存的(例如sum(a))字段。
        resourceCondition.getResourceColumns().remove(function.toString().toUpperCase());
    }
    
    @Override
    public void visit(InverseExpression inverseExpression) {
        LOG.trace("visit inverseExpression:");
        
    }
    
    @Override
    public void visit(JdbcParameter jdbcParameter) {
        LOG.trace("visit jdbcParameter:");
        
    }
    
    @Override
    public void visit(DoubleValue doubleValue) {
        LOG.trace("visit doubleValue:");
        
    }
    
    @Override
    public void visit(LongValue longValue) {
        LOG.trace("visit longValue:");
        
    }
    
    @Override
    public void visit(DateValue dateValue) {
        LOG.trace("visit dateValue:");
        
    }
    
    @Override
    public void visit(TimeValue timeValue) {
        LOG.trace("visit timeValue:");
        
    }
    
    @Override
    public void visit(TimestampValue timestampValue) {
        LOG.trace("visit timestampValue:");
        
    }
    
    @Override
    public void visit(Parenthesis parenthesis) {
        LOG.trace("visit parenthesis:" + parenthesis.toString());
        parenthesis.getExpression().accept(this);
    }
    
    @Override
    public void visit(StringValue stringValue) {
        LOG.trace("visit stringValue:");
        
    }
    
    @Override
    public void visit(Addition addition) {
        LOG.trace("visit addition:");
        
    }
    
    @Override
    public void visit(Division division) {
        LOG.trace("visit division:");
        
    }
    
    @Override
    public void visit(Multiplication multiplication) {
        LOG.trace("visit multiplication:");
        
    }
    
    @Override
    public void visit(Subtraction subtraction) {
        LOG.trace("visit subtraction:");
        
    }

    @Override
    public void visit(Column column) {
        LOG.trace("visit column:" + column);
    }
    
    @Override
    public void visit(CaseExpression caseExpression) {
        LOG.trace("visit caseExpression:");
        
    }
    
    @Override
    public void visit(WhenClause whenClause) {
        LOG.trace("visit whenClause:");
        
    }
    
    @Override
    public void visit(ExistsExpression existsExpression) {
        LOG.trace("visit existsExpression:");
        
    }
    
    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        LOG.trace("visit allComparisonExpression:");
        
    }
    
    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        LOG.trace("visit anyComparisonExpression:");
    }
    
    /* order by分割线 */
    
    @Override
    public void visit(OrderByElement orderByElement) {
        LOG.trace("visit orderByElement:" + orderByElement.toString());
        Document sortDoc = new Document();
        if (orderByElement.isAsc()) {
            sortDoc.put(orderByElement.getColumnReference().toString().toUpperCase(), 1);
        }
        else {
            sortDoc.put(orderByElement.getColumnReference().toString().toUpperCase(), -1);
        }
        resourceCondition.addSortCondition(sortDoc);
    }
    
    /* SelectItem 分割线 */
    @Override
    public void visit(AllColumns allColumns) {
        LOG.trace("visit allColumns:" + allColumns.toString());
        resourceCondition.getResourceColumns().clear();
    }
    
    @Override
    public void visit(AllTableColumns allTableColumns) {
        LOG.trace("visit allTableColumns:" + allTableColumns.toString());
        
    }
    
    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        LOG.trace("visit selectExpressionItem:" + selectExpressionItem.toString());
        resourceCondition.addResourceColumn(selectExpressionItem.toString().toUpperCase());
        selectExpressionItem.getExpression().accept(this);
    }
    
    /* 实现 ItemsListVisitor 分割线 */
    
    @Override
    public void visit(ExpressionList expressionList) {
        LOG.trace("visit expressionList:" + expressionList.toString());
        
        for (int i = 0; i < expressionList.getExpressions().size(); i++) {
            LOG.trace(expressionList.getExpressions().get(i).toString());
            Object value = CommonUtil.ObjectTypeConvert(expressionList.getExpressions().get(i));
            initem.add(value);
        }
        
    }
    
    //-----------------------自定义方法分割线----------------------------------//
    
    /**
     * 对range条件的合并,如果sql中为 a>0 and a<10 合并成a between 0 and 10,否则会直接用a<10替换a>0
     * <功能详细描述>
     * @param condition
     * @param type 
     * @param oldKey
     * @param oldValue
     * @see [类、类#方法、类#成员]
     */
    public static Document rangeCombine(Document condition, String type, String key, Object value) {
        
        Document nowCondition = new Document();
        double newStart = 0;
        double newEnd = Double.MAX_VALUE;
        double oldStart = 0;
        double oldEnd = Double.MAX_VALUE;
        //如果condition中已经有那个key并且类型不一样就需要合并
        if (condition.containsKey(key) == true && ((Document)condition.get(key)).containsKey(type) == false) {
            try {
                //强制转成Double,方便后面比较
                for (String oldType : ((Document)condition.get(key)).keySet()) {
                    Double oldValue = Double.parseDouble(String.valueOf(((Document)condition.get(key)).get(oldType)));
                    if (oldType.equals("$gt"))
                        oldStart = oldValue + 0.0001;
                    else if (oldType.equals("$lt"))
                        oldEnd = oldValue - 0.0001;
                    else if (oldType.equals("$gte"))
                        oldStart = oldValue;
                    else if (oldType.equals("$lte"))
                        oldEnd = oldValue;
                }
               
                Double newValue = Double.parseDouble(value.toString());
                if (type.equals("$gt"))
                    newStart = newValue + 0.0001;
                else if (type.equals("$lt"))
                    newEnd = newValue - 0.0001;
                else if (type.equals("$gte"))
                    newStart = newValue;
                else if (type.equals("$lte"))
                    newEnd = newValue;
                
                //进行合并的判断
                if (oldStart > newStart)
                    newStart = oldStart;
                
                if (oldEnd < newEnd)
                    newEnd = oldEnd;
                
                nowCondition = new Document(key, new Document("$gte", newStart).append("$lte", newEnd));
                
            }
            catch (NumberFormatException ex) {
                nowCondition = new Document(key, new Document(type, CommonUtil.ObjectTypeConvert(value)));
            }
            
        }
        else {
            nowCondition = new Document(key, new Document(type, CommonUtil.ObjectTypeConvert(value)));
        }
        
        return nowCondition;
        
    }
}
