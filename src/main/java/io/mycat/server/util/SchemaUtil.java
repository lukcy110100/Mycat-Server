package io.mycat.server.util;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;

import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by magicdoom on 2016/1/26.
 */
public class SchemaUtil
{
    private static final Logger logger = LoggerFactory.getLogger(SchemaUtil.class);
    public static List<SchemaInfo> parseSchemas(String sql)
    {
        SQLStatement statement;
        SQLStatementParser parser = new MySqlStatementParser(sql);
        try {
            statement = parser.parseStatement();
            return parseTables(statement, new MycatSchemaStatVisitor());
        } catch (Exception t) {
            logger.error("SchemaUtil parseSchemas exception:{}",t);
        }
        return null;
    }

    public static SchemaInfo parseSchema(String sql)
    {
        SQLStatementParser parser = new MySqlStatementParser(sql);
      return parseTable(parser.parseStatement(),new MycatSchemaStatVisitor()  );
    }
    public static String detectDefaultDb(String sql, int type)
    {
        String db=null;
        Map<String, SchemaConfig> schemaConfigMap = MycatServer.getInstance().getConfig()
                .getSchemas();
        if(ServerParse.SELECT==type || ServerParse.INFORMATION_SCHEMA_COLUMNS == type || ServerParse.INFORMATION_SCHEMA_TABLES == type )
        {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
            if ((schemaInfo==null||schemaInfo.table==null)&&!schemaConfigMap.isEmpty())
            {
                db = schemaConfigMap.entrySet().iterator().next().getKey();
            }
            
            if (schemaInfo != null && schemaInfo.schema != null ) {
            	
				if ( schemaConfigMap.containsKey(schemaInfo.schema) ) {
					db = schemaInfo.schema;			
					
				/**
				 * 对 MySQL 自带的元数据库 information_schema 进行返回
				 */
				} else if ( "information_schema".equalsIgnoreCase( schemaInfo.schema ) ) {
					db = "information_schema";					
				}
			}
        }
        else
        if(ServerParse.INSERT==type||ServerParse.UPDATE==type||ServerParse.DELETE==type||ServerParse.DDL==type)
        {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
            if(schemaInfo!=null&&schemaInfo.schema!=null&&schemaConfigMap.containsKey(schemaInfo.schema)  ) {
                db = schemaInfo.schema;
            }
        }   else
        if((ServerParse.SHOW==type||ServerParse.USE==type||ServerParse.EXPLAIN==type||ServerParse.SET==type
                ||ServerParse.HELP==type||ServerParse.DESCRIBE==type)
                && !schemaConfigMap.isEmpty())
        {
            //兼容mysql gui  不填默认database
                db = schemaConfigMap.entrySet().iterator().next().getKey();
        }
        return db;
    }


    public static String parseShowTableSchema(String sql)
    {
        Matcher ma = pattern.matcher(sql);
        if(ma.matches()&&ma.groupCount()>=5)
        {
          return  ma.group(5);
        }
        return null;
    }

    public static SchemaInfo parseTable(SQLStatement stmt, SchemaStatVisitor schemaStatVisitor)
    {

                stmt.accept(schemaStatVisitor);
                String key = schemaStatVisitor.getCurrentTable();
                if (key != null && key.contains("`"))
                {
                    key = key.replaceAll("`", "");
                }

                if (key != null)
                {
                    SchemaInfo schemaInfo=new SchemaInfo();
                    int pos = key.indexOf(".");
                    if (pos > 0)
                    {
                        schemaInfo.schema=key.substring(0,pos);
                        schemaInfo.table=key.substring(pos+1);
                    }  else
                    {
                        schemaInfo.table=key;
                    }
                    return schemaInfo;
                }

        return null;
    }


    private static List<SchemaInfo> parseTables(SQLStatement stmt, SchemaStatVisitor schemaStatVisitor)
    {
        List<SchemaInfo> schemaInfos = new ArrayList<>();
        stmt.accept(schemaStatVisitor);

        if (schemaStatVisitor.getAliasMap() != null)
        {
            for (Map.Entry<String, String> entry : schemaStatVisitor.getAliasMap().entrySet())
            {
                String key = entry.getKey();
                if (key != null && key.contains("`"))
                {
                    key = key.replaceAll("`", "");
                }
                if (key != null)
                {
                    SchemaInfo schemaInfo = new SchemaInfo();
                    int pos = key.indexOf(".");
                    if (pos > 0)
                    {
                        schemaInfo.schema = key.substring(0, pos);
                        schemaInfo.table = key.substring(pos + 1);
                    }
                    else
                    {
                        schemaInfo.table = key;
                    }
                    schemaInfos.add(schemaInfo);
                }

            }
        }
        return schemaInfos;
    }

    public static  class SchemaInfo
    {
        public    String table;
        public    String schema;

        @Override
        public String toString()
        {
            final StringBuffer sb = new StringBuffer("SchemaInfo{");
            sb.append("table='").append(table).append('\'');
            sb.append(", schema='").append(schema).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

private  static     Pattern pattern = Pattern.compile("^\\s*(SHOW)\\s+(FULL)*\\s*(TABLES)\\s+(FROM)\\s+([a-zA-Z_0-9]+)\\s*([a-zA-Z_0-9\\s]*)", Pattern.CASE_INSENSITIVE);

    public static void main(String[] args)
    {

        String sql = "SELECT name, type FROM `mysql`.`proc` as xxxx WHERE Db='base'";
     //   System.out.println(parseSchema(sql));
        sql="insert into aaa.test(id) values(1)" ;
       // System.out.println(parseSchema(sql));
        sql="update updatebase.test set xx=1 " ;
        //System.out.println(parseSchema(sql));
        sql="CREATE TABLE IF not EXISTS  `test` (\n" + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "  `sid` bigint(20) DEFAULT NULL,\n" + "  `name` varchar(45) DEFAULT NULL,\n"
                + "  `value` varchar(45) DEFAULT NULL,\n"
                + "  `_slot` int(11) DEFAULT NULL COMMENT '自动迁移算法slot,禁止修改',\n" + "  PRIMARY KEY (`id`)\n"
                + ") ENGINE=InnoDB AUTO_INCREMENT=805781256930734081 DEFAULT CHARSET=utf8";
        System.out.println(parseSchema(sql));
        String pat3 = "show  full  tables from  base like ";
        Matcher ma = pattern.matcher(pat3);
        if(ma.matches())
        {
            System.out.println(ma.groupCount());
            System.out.println(ma.group(5));
        }



    }
}
