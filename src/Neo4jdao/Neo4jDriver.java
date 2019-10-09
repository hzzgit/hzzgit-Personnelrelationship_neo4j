package Neo4jdao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.log4j.Logger;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import ThreadPool.mainThread;


public class Neo4jDriver {
  // 驱动程序对象是线程安全的，通常是在应用程序范围内提供的。
  Driver driver;
  private static Logger logger = Logger.getLogger(mainThread.class);

  public Neo4jDriver(String uri, String user, String password) {
    driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
  }

  /**
   * 用于创建节点（单一）
   */
  public void createnode(String cql) {

    // 会话是轻量级的和一次性的连接包装器。
    try (Session session = driver.session()) {
      // Wrapping Cypher in an explicit transaction provides atomicity
      // and makes handling errors much easier.

      try (Transaction tx = session.beginTransaction()) {
        // tx.run("MERGE (a:Person {name: {x}})", parameters("x", name));
        tx.run(cql);
        tx.success();

      } catch (Exception e) {

        // e.printStackTrace();
        // TODO: handle exception
      }
    } catch (Exception e) {
      // logger.error("重复");
      // e.printStackTrace();
      // TODO: handle exception
    }
    // close();
  }

  /**
   * 进行集合的分割，每一万执行一次提交,节点分割提交，创建节点和关系线,单一提交方式
   * 
   * @param cqls
   */
  public void createnodemore(Collection<String> cqls) {
    // System.out.println(cqls.size());
    ArrayList<String> cqls2 = new ArrayList<String>();// 临时存放一万条集合的中转站
    // if(cqls.size()<5000){//如果小于5000就直接执行
    for (String string : cqls) {
      createnode(string);

    }
    // createnodemore1(cqls);
    // }else{//如果大于5000就分批
    // for (String string : cqls) {
    // cqls2.add(string);
    // if(cqls2.size()==5000){
    // for (String string2 : cqls2) {
    // createnode(string2);
    // }
    // cqls2=new ArrayList<String>();
    // }
    // }
    // }


  }


  /**
   * 进行集合的分割，每一万执行一次提交,节点分割提交，创建节点和关系线,批量提交方式
   * 
   * @param cqls
   */
  public void createnodemorepiliang(Collection<String> cqls) {
    System.out.println(cqls.size());
    ArrayList<String> cqls2 = new ArrayList<String>();// 临时存放一万条集合的中转站
    if (cqls.size() < 1000) {// 如果小于1000就直接执行
      createnodemore1(cqls);
    } else {// 如果大于1000就分批
      for (String string : cqls) {
        cqls2.add(string);
        if (cqls2.size() == 1000) {

          createnodemore1(cqls2);
          cqls2 = new ArrayList<String>();
        }
      }
      if (cqls2.size() > 0) {

        createnodemore1(cqls2);
      }
    }


  }

  /**
   * 用于创建节点（批量）,中转环节，真正执行在这里 语法：创建节点 CREATE (dept:Dept {
   * deptno:10,dname:"Accounting",location:"Hyderabad" }) 创建关联也可以
   */
  public void createnodemore1(Collection<String> cqls) {
    // 会话是轻量级的和一次性的连接包装器。
    try (Session session = driver.session()) {
      // Wrapping Cypher in an explicit transaction provides atomicity
      // and makes handling errors much easier.
      try (Transaction tx = session.beginTransaction()) {
        for (String string : cqls) {
          tx.run(string);
        }
        tx.success();

      } catch (Exception e) {
        // logger.error("节点重复");
        e.printStackTrace();
      }
    } catch (Exception e) {
      e.printStackTrace();
      // TODO: handle exception
    }

  }


  /**
   * 根据cql语句进行查询节点数据
   * 
   * @param cql
   * @return
   */
  public boolean checknodes(String cql) {
    boolean arg = false;
    try {
      Session session = driver.session();
      StatementResult result = session.run(cql);

      if (result.hasNext()) {
        arg = true;
      }

    } catch (Exception e) {
      e.printStackTrace();
      // TODO: handle exception
    }
    return arg;
  }

  /**
   * 根据cql语句进行查询节点，关系线等数据
   * 
   * @param cql
   * @return
   */
  public Map<String, HashSet<Map<String, Object>>> printJSON(String cql) {
    Map<String, HashSet<Map<String, Object>>> retuMap =
        new HashMap<String, HashSet<Map<String, Object>>>();
    try {
      Session session = driver.session();
      StatementResult result = session.run(cql);
      HashSet<Map<String, Object>> nodedatas = new HashSet<Map<String, Object>>();// 存放所有的节点数据
      HashSet<Map<String, Object>> allrelationships = new HashSet<Map<String, Object>>();// 存放所有的节点数据

      while (result.hasNext()) {
        Record record = result.next();
        Map<String, Object> date = record.asMap();// 这里面存的是这个关系的键值对，其实就是起始节点，关系，结束节点
        for (String key : date.keySet()) {
          Object object = date.get(key);
          InternalPath data = (InternalPath) object;// 强制转换,这个地方要注意查询出来的是什么，
          // 如果查询的结果是节点+关系，那么就强制转换这个,如果查询出来只有节点那么就是Node，关系则是Relationship
          Iterable<Node> allnodes = data.nodes();
          for (Node node : allnodes) {
            long nodeid = node.id();
            Map<String, Object> nodedatamap = new HashMap<String, Object>();
            Map<String, Object> data1 = node.asMap();// 添加节点的属性
            for (String key1 : data1.keySet()) {
              nodedatamap.put(key1, data1.get(key1));
            }
            nodedatamap.put("name", nodeid);
            nodedatas.add(nodedatamap);
          }
          Iterable<Relationship> relationships = data.relationships();
          Map<String, Object> shipdata = new HashMap<String, Object>();
          for (Relationship relationship : relationships) {
            Map<String, Object> data1 = relationship.asMap();// 添加关系的属性
            for (String key1 : data1.keySet()) {
              shipdata.put(key1, data1.get(key1));
            }
            long source = relationship.startNodeId();// 起始节点id
            long target = relationship.endNodeId();// 结束节点Id
            shipdata.put("source", source);// 添加起始节点id
            shipdata.put("target", target);
          }
          allrelationships.add(shipdata);
        }
      }
      retuMap.put("nodes", nodedatas);
      retuMap.put("relation", allrelationships);

    } catch (Exception e) {
      e.printStackTrace();
      // TODO: handle exception
    } finally {
      // close();
    }
    return retuMap;
  }


  public void close() {
    // Closing a driver immediately shuts down all open connections.
    driver.close();
  }

}
