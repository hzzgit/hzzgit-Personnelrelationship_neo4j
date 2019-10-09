package Neo4jdao;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

import org.apache.log4j.Logger;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementRunner;
import org.neo4j.driver.v1.Transaction;

import ThreadPool.mainThread;
import tools.Node4jtool;

public class neotest {
    // 驱动程序对象是线程安全的，通常是在应用程序范围内提供的。
    Driver driver;
	private static Logger logger = Logger.getLogger(mainThread.class); 

	public neotest(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
	}
	
	
	/**
	 * 查询节点
	 */
	public void selectnode(){
		  ArrayList relationship = new ArrayList();
		try (Session session=driver.session()){
		     StatementResult result = session.run("MATCH (p:人员)-[r]->(p1:人员) where p.身份证号码<>p1.身份证号码  RETURN r.规则 as relationship,p.身份证号码 as source,p.身份证号码 as target");
	            while (result.hasNext()) {
	                Record record = result.next();
	                relationship.add(record.get("relationship").asString());	               
	            }
		} catch (Exception e) {
			// TODO: handle exception
		}
	
	}
	
	public static void main(String[] args) {
		
		Neo4jDrivertest neo4jDriver=new Neo4jDrivertest("bolt://192.168.16.241:7687", "neo4j", "tch123456");
		String cql="MATCH ( p1:Persons) where p1.sfzh='450502198210220811' MATCH ( p2:Persons)  match p=(p1)<-[r]->(p2)  RETURN p  ";
		Map<String, HashSet<Map<String, Object>>> retuMap=neo4jDriver.printJSON(cql);
		HashSet<Map<String, Object>> nodes=retuMap.get("nodes");
		HashSet<Map<String, Object>> relations=retuMap.get("relation");
		for (Map<String, Object> map : relations) {
			System.out.println(map);
		}
		System.out.println("节点");
		for (Map<String, Object> map : nodes) {
			System.out.println(map);
		}

	
		
//	 	long startTime=System.currentTimeMillis();//记录结束时间  	        
//	 	String cql="LOAD CSV WITH HEADERS  FROM\"file:///test.csv\"  AS line      MERGE (p:person{sfzh:line.sfzh,xm:line.xm,xb:line.xb})";	
//
//
//	 	neo4jDriver.createnode(cql);
//		long endTime=System.currentTimeMillis();//记录结束时间  	        
//	    float excTime=(float)(endTime-startTime)/1000;  
//	    System.out.println("执行时间："+excTime+"s");
		
       
	}
}
