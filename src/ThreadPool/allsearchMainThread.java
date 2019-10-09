package ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.log4j.Logger;
import tools.Node4jtool;
import util.util;
import Controller.ReadController;
import beanpojo.t_neo4j_node_info_node;
import been.T_neo4j_link_judge;
import been.T_neo4j_link_property;
import been.T_neo4j_node_property;
import dao.T_neo4j_link_judge_dao;
import dao.T_neo4j_link_property_dao;
import dao.T_neo4j_node_property_dao;
import dao.sysdao;

/**
 * 用于全量抽取的情况的抽取的主线程
 * 
 * @author Administrator
 * 
 */
public class allsearchMainThread implements Runnable {

  private static Logger logger = Logger.getLogger(allsearchMainThread.class);


  // dao关系过滤条件
  private T_neo4j_link_judge_dao t_neo4j_link_judge_dao = new T_neo4j_link_judge_dao();

  // 节点属性dao
  private T_neo4j_node_property_dao t_neo4j_node_property_dao = new T_neo4j_node_property_dao();
  // 关系属性dao
  private T_neo4j_link_property_dao t_neo4j_link_property_dao = new T_neo4j_link_property_dao();

  // 非bean系统dao
  private sysdao sysdao = new sysdao();

  private t_neo4j_node_info_node startinfonode = new t_neo4j_node_info_node(); // 开始节点
  private t_neo4j_node_info_node endinfonode = new t_neo4j_node_info_node(); // 结束节点

  private String linkid; // 关联主键Id
  private String linkname; // 关联名
  private String aspect_type; // 1,单向，2,双向
  private String startid; // 起始节点id
  private String startusername; // 起始用户名
  private String starttablename; // 起始表名
  private String startnodename; // 起始节点名
  private String startlabalname; // 起始标签名
  private String startuniquename; // 起始唯一性字段
  private String endid; // 结束节点id
  private String endusername; // /结束用户名
  private String endtablename; // 结束表名
  private String endnodename; // 结束节点名
  private String endlabalname; // 结束标签名
  private String enduniquename; // 起始唯一性字段
  private String zlname; // 增量字段，只考虑开始表
  private String zltype; // 增量字段类型，只考虑开始表
  private String zlval; // 增量值，只考虑开始表

  // 获取到起始节点的属性
  private ArrayList<T_neo4j_node_property> startnodepro = new ArrayList<T_neo4j_node_property>();
  // 获取到结束节点的属性
  private ArrayList<T_neo4j_node_property> endnodepro = new ArrayList<T_neo4j_node_property>();

  // 获取到关系线的属性
  private ArrayList<T_neo4j_link_property> linkpro = new ArrayList<T_neo4j_link_property>();

  private ArrayList<T_neo4j_link_judge> judges = new ArrayList<T_neo4j_link_judge>(); // 条件过滤集合

  // 传入的数据，用于多线程全量抽取
  private ArrayList startdates = new ArrayList();


  public allsearchMainThread(String linkid, t_neo4j_node_info_node startinfonode,
      t_neo4j_node_info_node endinfonode, String linkname, String aspect_type,
      ArrayList startdates, ArrayList<T_neo4j_link_judge> judges,
      ArrayList<T_neo4j_node_property> startnodepro, ArrayList<T_neo4j_node_property> endnodepro,
      ArrayList<T_neo4j_link_property> linkpro) {
    // this.Neo4jDriver1=(Neo4jDriver)
    // Node4jtool.nodedridate.get(driverid);//获取驱动
    this.linkid = linkid;
    this.linkname = linkname;
    this.aspect_type = aspect_type;
    this.startinfonode = startinfonode;
    this.endinfonode = endinfonode;

    this.startid = startinfonode.getId();
    this.startusername = startinfonode.getUser_name();
    this.starttablename = startinfonode.getTable_name();
    this.startnodename = startinfonode.getNode_name();
    this.startlabalname = startinfonode.getLable_name();
    this.startuniquename = startinfonode.getUnique_field();
    this.endid = endinfonode.getId();
    this.endusername = endinfonode.getUser_name();
    this.endtablename = endinfonode.getTable_name();
    this.endnodename = endinfonode.getNode_name();
    this.endlabalname = endinfonode.getLable_name();
    this.enduniquename = endinfonode.getUnique_field();
    this.zlname = startinfonode.getBulking_field();
    this.zltype = startinfonode.getBulking_type();
    this.zlval = startinfonode.getBulking_val();
    this.startdates = startdates;
    this.judges = judges;
    this.startnodepro = startnodepro;
    this.endnodepro = endnodepro;
    this.linkpro = linkpro;
  }

  @Override
  public void run() {

    // TODO Auto-generated method stub
    try {
      biduistatoend(startdates, judges);
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(e);
    } finally {
      // Neo4jDriver1.close();//销毁
    }
  }


  /**
   * 进行起始表和结果表的数据相关性比对
   */
  private void biduistatoend(ArrayList startdates, ArrayList<T_neo4j_link_judge> judges)
      throws Exception {
    boolean arg = true;
    long startTime = System.currentTimeMillis();// 记录结束时间
    HashSet<String> neodatealladd = new HashSet<String>();// 总的添加节点集合
    ArrayList<String> linkdatealladd = new ArrayList<String>();// 总的添加关系线集合
    for (Object object : startdates) {
      java.util.Map startmap = (java.util.Map) object;// 获取到起始表单一数据
      boolean checkarg = isCheckwhere(startmap, judges);// 判断所有的条件是否有脏数据，也就是查询条件是否有空的
      if (checkarg) {
        String searchsql = getsearchwhere(startmap, judges);// 查询条件
        ArrayList dates = new ArrayList();
        System.out.println(searchsql);
        dates = sysdao.getbiduidates(searchsql);// 获取到比对的结果数据集合
        System.out.println(dates);
        java.util.Map dateMap = neonodeadd(startmap, dates, startid, endid);
        HashSet nodedates = (HashSet) dateMap.get("nodedates");// 节点数据获取
        ArrayList linkdates = (ArrayList) dateMap.get("linkdates");// 线数据获取
        if (nodedates.size() > 0) {
          neodatealladd.addAll(nodedates);// 先进行节点的添加
        }
        if (linkdates.size() > 0) {
          linkdatealladd.addAll(linkdates);// 线段数据添加
        }
      }
    }
    logger.debug(linkname + "开始节点的数量" + startdates.size());
    logger.debug(linkname + "要进行添加的节点数" + neodatealladd.size());
    logger.debug(linkname + "要进行添加的关联数" + linkdatealladd.size());

    System.out.println(linkname + "开始节点的数量" + startdates.size());
    System.out.println(linkname + "要进行添加的节点数" + neodatealladd.size());
    System.out.println(linkname + "要进行添加的关联数" + linkdatealladd.size());

    // 这里是计算总数
    util.setchulidate(startdates.size());
    util.setneedaddnode(neodatealladd.size());
    util.setneedaddlink(linkdatealladd.size());

    Thread thread = new Thread(new addNeoThread(neodatealladd, linkdatealladd));// 批量添加结点数据的线程

    ReadController.executorService2.submit(thread);

    // Neo4jDriver1.createnodemore(neodatealladd);//进行结点批量添加
    long endTime = System.currentTimeMillis();// 记录结束时间
    float excTime = (float) (endTime - startTime) / 1000;
    System.out.println("执行时间：" + excTime + "s");
    logger.debug("执行时间：" + excTime + "s");

  }

  /**
   * neo的节点添加
   */
  public java.util.Map neonodeadd(java.util.Map startmap, ArrayList dates, String startid,
      String endid) throws Exception {

    String startnodedate = "";// 开始节点拼接字符串
    String startunival = (String) startmap.get(startuniquename.toUpperCase());// 开始节点唯一性字段值
    // String satrtsfzh=(String) startmap.get("sfzh");//开始节点身份证号码
    for (T_neo4j_node_property t_neo4j_node_property : startnodepro) {
      String oraclefield = t_neo4j_node_property.getOracle_field();// oracle字段名
      String neofield = t_neo4j_node_property.getNeo4j_name();// neo属性名
      String unllallow = t_neo4j_node_property.getUnll_allow();// 是否为空
      String neoval = (String) startmap.get(oraclefield.toUpperCase());// 节点属性值
      if (neoval != null && !"".equals(neoval)) {
        startnodedate += neofield + ":'" + neoval + "',";
      }
    }
    System.err.println("报错点" + startnodedate);
    startnodedate = startnodedate.substring(0, startnodedate.length() - 1);// 进行开始节点的插入
    String startnode = Node4jtool.getnodecql(startnodename, startlabalname, startnodedate);
    // 这边是结束节点拼接
    HashSet<String> endnodedates = new HashSet<String>();// 结束节点集合
    ArrayList<String> linksdates = new ArrayList<String>();// 关联线集合
    String endnodedate = "";// 结束节点拼接
    String linkdate = "";// 关联线拼接
    for (Object object : dates) {
      endnodedate = "";
      linkdate = "";
      java.util.Map date = (java.util.Map) object;// 获取到每一条要进行添加的结束节点
      // String endsfzh=(String) date.get("sfzh");//结束节点身份证号码
      for (T_neo4j_node_property t_neo4j_node_property : endnodepro) {
        String oraclefield = t_neo4j_node_property.getOracle_field();// oracle字段名
        String neofield = t_neo4j_node_property.getNeo4j_name();// neo属性名
        String unllallow = t_neo4j_node_property.getUnll_allow();// 是否为空
        String neoval = (String) date.get(oraclefield.toUpperCase());// 节点属性值
        if (neoval != null && !"".equals(neoval)) {
          endnodedate += neofield + ":'" + neoval + "',";
        }
      }
      endnodedate = endnodedate.substring(0, endnodedate.length() - 1);
      String endnode = Node4jtool.getnodecql(endnodename, endlabalname, endnodedate);
      endnodedates.add(endnode);// 添加结束节点

      String endunival = (String) date.get(enduniquename.toUpperCase());// 结束节点唯一字段值
      for (T_neo4j_link_property link : linkpro) {
        String oraclefieldlink = link.getOracle_field();// 关联线Oralce字段名
        String neofieldlink = link.getNeo4j_name();// 关联线neo字段名
        String neovallink = (String) date.get(oraclefieldlink.toUpperCase());// 属性值
        if (neovallink == null) {
          neovallink = "";
        }
        linkdate += neofieldlink + ":'" + neovallink + "',"; // 拼接属性和值的Json

      }
      // linkdate=linkdate.substring(0,linkdate.length()-1);
      linkdate += "gz:'" + linkname + "'";
      String linksd =
          Node4jtool.getrelationship(startuniquename, enduniquename, startlabalname, endlabalname,
              startunival, endunival, linkname, linkdate, aspect_type);
      // System.out.println(linksd);
      linksdates.add(linksd);// 关联线集合添加

    }
    endnodedates.add(startnode);// 添加开始节点

    java.util.Map datesss = new HashMap();
    datesss.put("nodedates", endnodedates);// 节点数剧
    datesss.put("linkdates", linksdates);// 线数据

    return datesss;

  }

  /**
   * 检测拼接的条件是否全部不为空，必须全部不为空，才可以继续进行添加节点和关系的操作
   * 
   * @return true 全部不为空,false有一个为空
   */
  public boolean isCheckwhere(java.util.Map startmap, ArrayList<T_neo4j_link_judge> judges) {
    boolean arg = true;
    for (T_neo4j_link_judge t_neo4j_link_judge : judges) {// 遍历条件
      String leftfield = t_neo4j_link_judge.getLeft_field();// 左边
      String leftval = (String) startmap.get(leftfield.toUpperCase());// 获取到条件的值
      if (leftval == null || "".equals(leftval)) {// 只要有一个值是空的就是脏数据
        arg = false;
      }
    }
    return arg;
  }

  /**
   * \ 拼接查询条件
   * 
   * @param judges
   * @return
   */
  private String getsearchwhere(java.util.Map startmap, ArrayList<T_neo4j_link_judge> judges)
      throws Exception {

    String searchwhere = "select * from " + endusername + "." + endtablename + " a  where 1=1 ";
    for (T_neo4j_link_judge t_neo4j_link_judge : judges) {// 遍历条件
      String datetype = t_neo4j_link_judge.getData_type();// 字段类型
      String judgetype = t_neo4j_link_judge.getJudge_type();// 是否关联自身
      String symbol = t_neo4j_link_judge.getSymbol();// 条件拼接
      String add_val = t_neo4j_link_judge.getAdd_value();// 加权条件值
      String leftfield = t_neo4j_link_judge.getLeft_field();// 左边
      String rightfield = t_neo4j_link_judge.getRight_field();// 右边
      String leftval = (String) startmap.get(leftfield.toUpperCase());// 获取到条件的值

      if (!"".equals(leftval) && leftval != null) {
        if ("1".equals(judgetype)) {// 关联自身
          if ("1".equals(symbol)) {// 假如是>=
            searchwhere += " and  " + rightfield + " >= '" + leftval + "' ";
          } else if ("2".equals(symbol)) {// 假如是<=
            searchwhere += " and  " + rightfield + " <= '" + leftval + "' ";
          } else if ("3".equals(symbol)) {// 假如是=
            searchwhere += " and  " + leftfield + " = '" + leftval + "' ";
          } else if ("4".equals(symbol)) {// 假如是!=
            searchwhere += " and  " + rightfield + " <> '" + leftval + "' ";
          } else if ("5".equals(symbol)) {// 假如是<>
            if ("date".equals(datetype.toLowerCase())) {// 假如是时间
              leftval = leftval.substring(0, leftval.length() - 2);
              searchwhere +=
                  " and  " + rightfield + "  >= to_date('" + leftval
                      + "','yyyy-mm-dd hh24:mi:ss')-" + add_val + " and  " + rightfield
                      + " <= to_date('" + leftval + "','yyyy-mm-dd hh24:mi:ss')+" + add_val + " ";
            } else {// 如果是其他
              searchwhere +=
                  " and  " + rightfield + " between " + leftval + "-" + add_val + " and " + leftval
                      + "+" + add_val + "   ";
            }
          }
        } else {// 排除自身
          if (rightfield != null && !"".equals(rightfield)) {// 不为空不为Null
            // searchwhere+=" and   "+rightfield+" <> '"+leftval+"' ";
            searchwhere +=
                " and   not exists ( select 1  from " + endusername + "." + endtablename
                    + "  b where a." + rightfield + "=b." + rightfield + "  and " + rightfield
                    + " = '" + leftval + "' ) ";
          } else {
            // searchwhere+=" and  "+leftfield+" <> '"+leftval+"' ";
            searchwhere +=
                " and   not exists ( select 1  from " + endusername + "." + endtablename
                    + "  b where a." + leftfield + "=b." + leftfield + "  and " + leftfield
                    + " = '" + leftval + "' ) ";

          }

        }
      }

    }
    return searchwhere;
  }

}
