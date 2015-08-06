package com.mongosql.utils;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;

/**
 * 
 * 工具类
 * @author  Administrator
 * @version  [版本号, 2015年7月4日]
 * @see  [相关类/方法]
 * @since  [产品/模块版本]
 */
public class CommonUtil {
    
    public static ConcurrentHashMap<String, String> sendertable = new ConcurrentHashMap<String, String>();
    
    public static final Logger LOG = LoggerFactory.getLogger(CommonUtil.class);
    
    /**
     * 获得字符串边界
     * <功能详细描述>
     * @param content
     * @param beginStr
     * @param endStr
     * @return
     * @see [类、类#方法、类#成员]
     */
    public static String getStringByBorder(String content, String beginStr, String endStr) {
        int i = content.indexOf(beginStr);
        int j = content.indexOf(endStr);
        return content.substring(i, j);
    }
    
    /**
     * 强制将string转成long,如果异常就返回字符串
     * <功能详细描述>
     * @param str
     * @return
     * @see [类、类#方法、类#成员]
     */
    public static Object convertStrToLong(String str) {
        Object l = null;
        try {
            l = Long.parseLong(str);
            return l;
        }
        catch (NumberFormatException ex) {
            l = str;
        }
        return l;
    }
    
    /**
     * 类型转换
     * @param value
     * @return Object obj
    */
    public static Object ObjectTypeConvert(Object value) {
        Object obj = null;
        if (value instanceof StringValue) {
            //字符串的'1'，需要强制转换成long型的 1
            obj = convertStrToLong(prString(value.toString()));
        }
        else if (value instanceof LongValue) {
            try {
                //可以转成int的数值就转成int
                obj = Integer.parseInt(value.toString());
            }
            catch (NumberFormatException ex) {
                obj = Long.parseLong(value.toString());
            }
        }
        else if (value instanceof NullValue)
            obj = null;
        else if (value instanceof DoubleValue)
            obj = Double.parseDouble(value.toString());
        return obj;
    }
    
    /**
     * 剔除字符串前面的单引号 ',
     * 
     * @param value
     * @return
     */
    public static String prString(String value) {
        char t = '\'';
        //去除开头和结尾的单引号
        if (value.length() > 2 && t == value.charAt(0) && t == value.charAt(value.length() - 1)) {
            value = value.substring(1, value.length() - 1);
        }
        return value;
    }
    
   
}
