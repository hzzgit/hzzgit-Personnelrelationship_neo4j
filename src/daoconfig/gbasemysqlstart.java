package daoconfig;

import java.util.ArrayList;

public class gbasemysqlstart {

  public gbasemysqlstart(String type) {
    config config1 = new config(type);
  }

  /**
   * 进行数据库查询
   * 
   * @return
   */
  public static ArrayList getresult(String sql, Object object2) {
    oracledb oracledb = new oracledb();
    ArrayList lists = (ArrayList) oracledb.searchnopagesqlclass(sql, object2);
    return lists;
  }

}
