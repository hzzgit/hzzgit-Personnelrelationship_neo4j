package tools;

import java.util.HashMap;
import java.util.Map;

/**
 * node4j工具箱
 * 
 * @author Administrator 删除节点和关系MATCH (cc:personinfo)-[关系]-(c:personinfo) DELETE cc,c,关系
 */
public class Node4jtool {

  public static Map nodedridate = new HashMap();// 用来存放node驱动池

  /**
   * 进行结点生成语句的拼接返回
   * 
   * @param nodename
   * @param date
   * @return
   */
  public static String getnodecql(String nodename, String lablename, String date) {
    String cql = "CREATE   (" + nodename + ":" + lablename + " { " + date + " })";
    return cql;
  }

  /**
   * 进行关联生成语句的拼接返回
   * 
   * @param startnodename ,关联的开始节点
   * @param endnodename,关联的结束节点
   * @param startsfzh,开始节点身份证号
   * @param endsfzh,结束节点身份证号
   * @param linkname,关联名,也就是拼音的两个结点之间的关系，例如同住关系等
   * @param shipdate,要插入的属性值
   * @return
   */
  public static String getrelationship(String startunifiled, String endunifiled,
      String startnodename, String endnodename, String startsfzh, String endsfzh, String linkname,
      String shipdate, String fangxiang) {
    String cql = "";
    if ("1".equals(fangxiang)) {// 单向
      cql =
          "match (p1:" + startnodename + ") where p1." + startunifiled + "='" + startsfzh
              + "' match (p2:" + endnodename + ") where p2." + endunifiled + "='" + endsfzh
              + "'   CREATE   (p1)-[:" + linkname + "{" + shipdate + "}]->(p2) ";
    } else if ("2".equals(fangxiang)) {// 反向
      cql =
          "match (p1:" + startnodename + ") where p1." + startunifiled + "='" + startsfzh
              + "' match (p2:" + endnodename + ") where p2." + endunifiled + "='" + endsfzh
              + "'   CREATE   (p1)<-[:" + linkname + "{" + shipdate + "}]-(p2) ";

    } else {// 双向
      cql =
          "match (p1:" + startnodename + ") where p1." + startunifiled + "='" + startsfzh
              + "' match (p2:" + endnodename + ") where p2." + endunifiled + "='" + endsfzh
              + "'   CREATE   (p1)<-[:" + linkname + "{" + shipdate + "}]->(p2) ";
    }
    return cql;
  }

  /**
   * 创建索引
   * 
   * @param lablename，标签名
   * @param propername,属性名
   * @return
   */
  public static String getcreateindex(String lablename, String propername) {
    String cql = "CREATE INDEX ON :" + lablename + " (" + propername + ")";
    return cql;
  }

  /**
   * 创建唯一性
   * 
   * @param lablename，标签名
   * @param propername,属性名
   * @return
   */
  public static String getcreateunique(String lablename, String propername) {
    String cql =
        "CREATE CONSTRAINT ON (cc:" + lablename + ") ASSERT cc." + propername + " IS UNIQUE";
    return cql;
  }

  /**
   * 修改节点的属性，如果节点发生更新
   * 
   * @return
   */
  public static String updatenode(String sfzh, String xm) {
    String cql = "MATCH (n:Persons {sfzh:'" + sfzh + "'}) \r\n" + "  SET n.xm='" + xm + "'  ";
    return cql;
  }
}
