package dao;

import java.util.ArrayList;
import java.util.Map;
import beanpojo.V_analysis_jtgx;
import daoconfig.oracledb;

/**
 * 亲属关系的dao层
 * 
 * @author Administrator
 * 
 */
public class family_dao {
  private oracledb oracledb = new oracledb();

  /**
   * 查询出对应人员的子女的信息
   * 
   * @return
   */
  public ArrayList<V_analysis_jtgx> checkzinv(String pid) {
    ArrayList<V_analysis_jtgx> lists = new ArrayList<V_analysis_jtgx>();
    String sql =
        "select t.hu_id,t.hu_id_new,t.master_relation,t.name, t.pid,gender, t.fa_person_id,t.fa_pid, t.fa_name, t.ma_person_id, t.ma_pid,t.ma_name, t.po_person_id, t.po_pid,t.po_name, t.gurardian_1_id, t.gurardian_1_pid,  t.gurardian_1, t.WARDSHIP_1, t.gurardian_2_id, t.gurardian_2_pid,t.gurardian_2, t.WARDSHIP_2,dob,nation,native_place,WHEN_OPERATED from tc_tools.v_analysis_jtgx t ";
    sql +=
        " where t.fa_pid = '" + pid + "' or t.ma_pid = '" + pid
            + "'      or t.gurardian_1_pid =  '" + pid + "'      or t.gurardian_2_pid = '" + pid
            + "'  ";
    // System.out.println(sql);
    lists = (ArrayList<V_analysis_jtgx>) oracledb.searchnopagesqlclass(sql, new V_analysis_jtgx());
    return lists;
  }

  /**
   * 查询父亲节点，母亲节点，监护人节点，是否大于15，大于15代表集体户，进行过滤
   * 
   * @return
   */
  public int checkqscount(String colname, String pid) {
    String sql =
        "select count(1) cn from tc_tools.v_analysis_jtgx where " + colname + "='" + pid + "'";
    String co = oracledb.getnosqloneval(sql);
    int cos = Integer.parseInt(co);
    return cos;
  }

  /**
   * 查询出整个户籍的人员关系
   * 
   * @return
   */
  public ArrayList<V_analysis_jtgx> checkhuji(String huid) {
    ArrayList<V_analysis_jtgx> lists = new ArrayList<V_analysis_jtgx>();
    String sql = "select * from tc_tools.v_analysis_jtgx where hu_id_new ='" + huid + "'";
    lists = (ArrayList<V_analysis_jtgx>) oracledb.searchnopagesqlclass(sql, new V_analysis_jtgx());
    return lists;
  }


  /**
   * 根据身份证号查询性别
   * 
   * @return
   */
  public String checkxbbyid(String pid) {
    String sex = "";
    String sql = "select gender from tc_tools.v_analysis_jtgx where pid ='" + pid + "'";
    ArrayList lists = (ArrayList) oracledb.searchnopagesqlobject(sql);
    if (lists.size() > 0) {
      Map a = (Map) lists.get(0);
      sex = (String) a.get("GENDER");

    }
    return sex;
  }

  /**
   * 获取起始表的数量
   * 
   * @return
   */
  public int getstarttablecount(String startuser, String starttable, String zlname, String zltype,
      String beginzlval, String endzlval) {
    ArrayList lists = new ArrayList();
    String sql =
        "SELECT  count(1) cn from (select distinct hu_id_new from " + startuser + "." + starttable
            + " where 1=1  ";
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

        // if (beginzlval.equals(endzlval)) {// 假如起始时间和结束时间一样的情况，那么只需要大于即可
        // sql += " and " + zlname + " >= to_date('" + beginzlval + "','yyyy-MM-dd HH24:mi:ss')  ";
        // } else {
        sql +=
            " and " + zlname + " > to_date('" + beginzlval + "','yyyy-MM-dd HH24:mi:ss')  and "
                + zlname + " <= to_date('" + endzlval + "','yyyy-MM-dd HH24:mi:ss') ";
        // }
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
    sql += " )";

    oracledb oracledb = new oracledb();
    lists = (ArrayList) oracledb.searchnopagesqlobject(sql);
    Map date = (Map) lists.get(0);

    int count = Integer.parseInt(String.valueOf(date.get("CN")));

    return count;
  }

  /**
   * 获取到起始表的分页数据
   * 
   * @return
   */
  public ArrayList getjudgedates(int page, int pagesize, String startuser, String starttable,
      String zlname, String zltype, String beginzlval, String endzlval) {
    ArrayList lists = new ArrayList();
    String sql = "";
    sql = "select distinct hu_id_new from " + startuser + "." + starttable + " where 1=1 ";
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
        // if (beginzlval.equals(endzlval)) {// 假如起始时间和结束时间一样的情况，那么只需要大于等于即可
        // sql += " and " + zlname + " >= to_date('" + beginzlval + "','yyyy-MM-dd HH24:mi:ss')  ";
        // } else {
        sql +=
            " and " + zlname + " > to_date('" + beginzlval + "','yyyy-MM-dd HH24:mi:ss')  and "
                + zlname + " <= to_date('" + endzlval + "','yyyy-MM-dd HH24:mi:ss') ";
        // }
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
}
