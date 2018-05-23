package io.mycat.route.parser.druid.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.stat.TableStat;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.DruidParser;
import io.mycat.route.parser.druid.DruidParserFactory;
import io.mycat.route.parser.druid.DruidShardingParseInfo;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.server.parser.ServerParse;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.alibaba.druid.sql.ast.expr.SQLBinaryOperator.*;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;

/**
 * sql解析单元测试
 * @author lian
 * @date 2016年12月2日
 */
public class DefaultDruidParserTest {
	
	private SchemaConfig schema;
	private DruidParser druidParser;
	@Before
	public void setUp(){
		
		schema = mock(SchemaConfig.class);
		druidParser = new DefaultDruidParser();
	}
	@Test
	public void testParser() throws Exception {
		Object[] aa ;
//                aa = getParseTables("SHOW KEYS FROM `customers` FROM `test`;");
		aa = getParseTables("select TABLE_SCHEMA,PRIVILEGE_TYPE from information_schema.SCHEMA_PRIVILEGES where  grantee haha ('\\'czn\\'@\\'%\\'') and grantee like 'haha';");
        aa = getParseTables("select TABLE_SCHEMA,PRIVILEGE_TYPE from information_schema.SCHEMA_PRIVILEGES fasdv ");
		assertArrayEquals(getParseTables("select id as id from company t;"), getArr("company".toUpperCase()));
		assertArrayEquals(getParseTables("select 1 from (select 1 from company) company;"), getArr("company".toUpperCase()));

//        System.out.println( aa[0]);
	}
	
	private Object[] getParseTables(String sql) throws Exception{

		String str = "select COLUMN_NAME, CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_TYPE from information_schema.columns where table_schema = 'TESTDB' and table_name = 'mdb_tbl_yaodh_0';";
		str = str.replaceAll("mdb_tbl_yaodh_\\d", "mdb_tbl_yaodh");

		MySqlStatementParser parser1 = new MySqlStatementParser(sql);
		SQLStatement stmt = parser1.parseStatement();


		MySqlSchemaStatVisitor visitor1 = new MySqlSchemaStatVisitor();
		stmt.accept(visitor1);

		String aa = String.format("'%s'@'%%'", "haha");
		boolean bb = "abc".matches("(.+)abc");
        bb = "babc".matches("(.+)abc");
        bb = "abc".matches("(.*)abc");
        bb = "ddabc".matches("(.*)abc(.*)");
        bb = "dabc".matches("(.*)abc");
        aa = "%abc%".replaceAll("%", "(.*)").replaceAll("_", "(.?)");
        bb = "ddabc".matches(aa);
        bb = "ddabc".matches(Optional.ofNullable("_abc%").toString().replaceAll("%", "(.*)").replaceAll("_", "(.?)").toString());
        bb = "abc".matches(Optional.ofNullable("_abc%").toString().replaceAll("%", "(.*)").replaceAll("_", "(.?)").toString());
        bb = "ddabc".matches("");


		String currentTable = visitor1.getCurrentTable();
        Collection<TableStat.Column> dd = visitor1.getColumns();
        List<TableStat.Condition> ll = visitor1.getConditions();
		
		SQLStatementParser parser = new MySqlStatementParser(sql);
		SQLStatement statement = parser.parseStatement();
        SQLSelectStatement selectStmt = (SQLSelectStatement)statement;
        SQLSelect ass = selectStmt.getSelect();
//        ass.getQuery().
        MycatSchemaStatVisitor visitor = new MycatSchemaStatVisitor();


        LayerCachePool cachePool = mock(LayerCachePool.class);

        RouteResultset rrs = new RouteResultset(sql, ServerParse.SHOW);


//		druidParser.visitorParse(rrs, statement, visitor);
		druidParser.parser(null, rrs, statement, sql, null, visitor);
        visitor.getCurrentTable();
        visitor.getConditions();

//		visitor.getConditions().get(0).getOperator();
		System.out.println(Equality.name);
		DruidShardingParseInfo ctx = druidParser.getCtx();
		return ctx.getTables().toArray();
	}


	
	private Object[] getArr(String...strings){
		return strings;
	}
}
