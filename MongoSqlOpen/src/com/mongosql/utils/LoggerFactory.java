package com.mongosql.utils;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * 日志工厂，返回一个日志的对象
 * @author  陈荣耀
 * @version  [版本号, 2015年7月23日]
 * @see  [相关类/方法]
 * @since  [产品/模块版本]
 */

public class LoggerFactory {
    
    static {
        PropertyConfigurator.configure("conf/log4j.properties");
    }
    
    public static Logger getLogger(Class<?> classname) {
        Logger LOG = Logger.getLogger(classname);
        return LOG;
    }
    
}
