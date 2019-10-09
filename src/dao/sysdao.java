package dao;

import java.util.ArrayList;
import java.util.Map;
import been.T_neo4j_link_judge;
import been.T_neo4j_node_property;
import daoconfig.oracledb;

/**
 * 特殊的查询情况
 * 
 * @author Administrator
 * 
 */
public class sysdao {
  /**
   * 查询表字段
   * 
   * @return
   */
  public ArrayList checkorderziduan() {
    ArrayList lists = new ArrayList();
    String sql =
        "SELECT  column_name FROM all_tab_cols WHERE table_name = 'V_YZB_ORDER' and data_type NOT IN ('NUMBER','DATE') ";
    oracledb oracledb = new oracledb();
    lists = (ArrayList) oracledb.searchnopagesqlobject(sql);
    return lists;
  }

  /**
   * 获取起始表的增量最大值
   * 
   * @return
   */
  public String getstarttablecount(String zlname, String zltype, String startuser, String starttable) {
    ArrayList lists = new ArrayList();
    String sql = "";
    if ("2".equals(zlname)) {// 如果是时间
      sql =
          "SELECT to_char(  max(" + zlname + "),'yyyy-MM-dd HH24:mi:ss') cn from " + startuser
              + "." + starttable + " ";
    } else {// 字符串时间
      sql = "SELECT  max(" + zlname + ") cn from " + startuser + "." + starttable + " ";
    }
    oracledb oracledb = new oracledb();
    lists = (ArrayList) oracledb.searchnopagesqlobject(sql);
    Map a = (Map) lists.get(0);
    String max = (String) a.get("CN");
    return max;
  }

  /**
   * 修改增量值
   */
  public void savezlval(String zltype, String zlval, String id) {
    String sql = "";
    if ("2".equals(zltype)) {
      sql =
          "update tc_tools.t_neo4j_node_info set bulking_val='"
              + zlval.substring(0, zlval.length() - 2) + "'  where id='" + id + "'";
    } else {
      sql =
          "update tc_tools.t_neo4j_node_info set bulking_val='" + zlval + "'  where id='" + id
              + "'";

    }
    oracledb oracledb = new oracledb();
    oracledb.executesql(sql);

  }

  /**
   * 获取起始表的数量
   * 
   * @return
   */
  public ArrayList getstarttablecount(String startuser, String starttable, String zlname,
      String zltype, String beginzlval, String endzlval) {
    ArrayList lists = new ArrayList();
    String sql = "SELECT  count(1) cn from " + startuser + "." + starttable + " where 1=1  ";
    if ("".equals(beginzlval) && !"".equals(endzlval)) {// 全量抽取
      if ("2".equals(zltype.toLowerCase())) {// 时间类型
        endzlval = endzlval.substring(0, endzlval.length() - 2);
        sql += " and " + zlname + " <= to_date('" + endzlval + "','yyyy-MM-dd HH24:mi:ss') ";
      } else if ("3".equals(zltype.toLowerCase())) {// 字符串时间类型
        sql +=
            " and to_date(" + zlname + ",'yyyy-MM-dd HH24:mi:ss') <= to_date('" + endzlval
                + "','yyyy-MM-dd HH24:mi:ss') ";
      } else {// 数字
        sql += " and " + zlname + " <= " + endzlval + " ";
      }
    } else {// 增量抽取
      if ("2".equals(zltype.toLowerCase())) {// 时间类型
        endzlval = endzlval.substring(0, endzlval.length() - 2);
        sql +=
            " and " + zlname + " > to_date('" + beginzlval + "','yyyy-MM-dd HH24:mi:ss')  and "
                + zlname + " <= to_date('" + endzlval + "','yyyy-MM-dd HH24:mi:ss') ";
      } else if ("3".equals(zltype.toLowerCase())) {// 字符串时间类型
        sql +=
            " and to_date(" + zlname + ",'yyyy-MM-dd HH24:mi:ss') > to_date('" + beginzlval
                + "','yyyy-MM-dd HH24:mi:ss')  and to_date(" + zlname
                + ",'yyyy-MM-dd HH24:mi:ss') <= to_date('" + endzlval
                + "','yyyy-MM-dd HH24:mi:ss') ";
      } else {// 数字
        sql += " and " + zlname + " > " + beginzlval + "  and " + zlname + " <= " + endzlval + "  ";
      }
    }
    oracledb oracledb = new oracledb();
    lists = (ArrayList) oracledb.searchnopagesqlobject(sql);

    return lists;
  }

  /**
   * 获取到起始表的分页数据
   * 
   * @return
   */
  public ArrayList getjudgedates(int page, int pagesize, String startuser, String starttable,
      ArrayList<T_neo4j_link_judge> judges, ArrayList<T_neo4j_node_property> startnodepro,
      String zlname, String zltype, String beginzlval, String endzlval) {
    ArrayList lists = new ArrayList();
    String coldate = "";
    for (T_neo4j_link_judge object : judges) {
      coldate += object.getLeft_field() + ",";
    }
    for (T_neo4j_node_property object : startnodepro) {
      boolean arg = true;
      for (T_neo4j_link_judge object1 : judges) {// 不能有重复列
        if (object1.getLeft_field().equals(object.getOracle_field())) {
          arg = false;
        }
      }
      if (arg) {
        coldate += object.getOracle_field() + ",";
      }

    }
    coldate = coldate.substring(0, coldate.length() - 1);
    String sql = "";
    sql = "select " + coldate + " from " + startuser + "." + starttable + " where 1=1 ";
    if ("".equals(beginzlval) && !"".equals(endzlval)) {// 全量抽取
      if ("2".equals(zltype.toLowerCase())) {// 时间类型
        endzlval = endzlval.substring(0, endzlval.length() - 2);
        sql += " and " + zlname + " <= to_date('" + endzlval + "','yyyy-MM-dd HH24:mi:ss') ";
      } else if ("3".equals(zltype.toLowerCase())) {// 字符串时间类型
        sql +=
            " and   to_date(" + zlname + ",'yyyy-MM-dd HH24:mi:ss') <= to_date('" + endzlval
                + "','yyyy-MM-dd HH24:mi:ss') ";
      } else {// 数字
        sql += " and " + zlname + " <= " + endzlval + " ";
      }
    } else {// 增量抽取
      if ("2".equals(zltype.toLowerCase())) {// 时间类型
        endzlval = endzlval.substring(0, endzlval.length() - 2);
        sql +=
            " and " + zlname + " > to_date('" + beginzlval + "','yyyy-MM-dd HH24:mi:ss')  and "
                + zlname + " <= to_date('" + endzlval + "','yyyy-MM-dd HH24:mi:ss') ";
      } else if ("3".equals(zltype.toLowerCase())) {// 字符串时间类型
        sql +=
            " and to_date(" + zlname + ",'yyyy-MM-dd HH24:mi:ss') > to_date('" + beginzlval
                + "','yyyy-MM-dd HH24:mi:ss')  and to_date(" + zlname
                + ",'yyyy-MM-dd HH24:mi:ss') <= to_date('" + endzlval
                + "','yyyy-MM-dd HH24:mi:ss') ";
      } else {// 数字
        sql += " and " + zlname + " > " + beginzlval + "  and " + zlname + " <= " + endzlval + "  ";
      }
    }
    oracledb oracledb = new oracledb();
    lists = (ArrayList) oracledb.searchpagesqlobject(sql, page, pagesize);
    return lists;
  }


  /**
   * 进行起始和结果表比对的全部数据,因为数量不大所有不分页
   * 
   * @return
   */
  public ArrayList getbiduidates(String sql) {
    ArrayList lists = new ArrayList();
    oracledb oracledb = new oracledb();
    lists = (ArrayList) oracledb.searchnopagesqlobject(sql);
    return lists;
  }
}
