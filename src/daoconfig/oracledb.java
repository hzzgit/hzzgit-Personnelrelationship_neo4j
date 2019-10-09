package daoconfig;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 进行oracle数据库查询
 */
public class oracledb {

  private PreparedStatement ps;
  private ResultSet rs;
  private Statement st;
  private Connection con = null;

  public void close() {
    try {
      if (rs != null) {
        rs.close();
        rs = null;
      }
      if (ps != null) {
        ps.close();
        ps = null;
      }
      if (con != null) {
        con.close();
        con = null;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }


  }

  /**
   * 返回指定class格式的内容,不分页
   * 
   * @param sql
   * @return
   */
  public Object searchnopagesqlclass(String sql, Object object2) {


    try {
      con = config.dataSource.getConnection();
    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    if (con != null) {
    } else {
    }

    ArrayList jsonArray = new ArrayList();
    try {
      ps = con.prepareStatement(sql);

      rs = ps.executeQuery();
      ResultSetMetaData rsmd = rs.getMetaData();
      int colnum = rsmd.getColumnCount();
      Class<? extends Object> c2 = object2.getClass();
      while (rs.next()) {
        Object object = c2.newInstance();// 创建新的对象
        Class<? extends Object> c = object.getClass();
        Field[] declaredFields = c.getDeclaredFields();// 获取所有的变量名
        for (int i = 0; i < declaredFields.length; i++) {
          Field field = declaredFields[i];
          String filename = field.getName();// 获取变量名
          filename = filename.substring(0, 1).toUpperCase() + filename.substring(1);
          Method methods2;
          try {
            methods2 = c.getMethod("set" + filename, String.class);// 注意参数不是String,是string 类型
            String colval = rs.getString(filename);
            methods2.invoke(object, colval);// 通过对象，调用有参数的方法
            // 如果这个地方需要持久保存，那么就是object类放进去。不然就是加上c.newInstance()
          } catch (Exception e) {
            methods2 = c.getMethod("set" + filename, int.class);// 注意参数不是String,是string 类型
            int colval = rs.getInt(i);
            methods2.invoke(object, colval);// 通过对象，调用有参数的方法
          }

        }
        jsonArray.add(object);
      }
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    finally {
      this.close();
    }
    // System.out.print(cols.toString());

    return jsonArray;
  }


  /**
   * 返回指定class格式的内容，带分页
   * 
   * @param sql
   * @return
   */
  public Object searchpagesqlclass(String sql, Object object2, int page, int pagesize) {


    try {
      con = config.dataSource.getConnection();
    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    if (con != null) {
    } else {
    }

    ArrayList jsonArray = new ArrayList();
    try {
      int pagesize1 = 0;
      int page1 = 0;
      pagesize1 = pagesize * page;
      page1 = ((page - 1) * pagesize) + 1;// 获取到其实的显示数量
      String sql1 = "select * from ( select sasasdasds.*,ROWNUM RN from (";
      sql1 += sql;
      sql1 += "  ) sasasdasds where ROWNUM<=" + pagesize1 + ") where RN>= " + page1;
      ps = con.prepareStatement(sql1);

      rs = ps.executeQuery();
      ResultSetMetaData rsmd = rs.getMetaData();
      int colnum = rsmd.getColumnCount();
      colnum = colnum - 1;
      Class<? extends Object> c2 = object2.getClass();
      while (rs.next()) {
        Object object = c2.newInstance();// 创建新的对象
        Class<? extends Object> c = object.getClass();
        Field[] declaredFields = c.getDeclaredFields();// 获取所有的变量名
        for (int i = 0; i < declaredFields.length; i++) {
          Field field = declaredFields[i];
          String filename = field.getName();// 获取变量名
          filename = filename.substring(0, 1).toUpperCase() + filename.substring(1);

          Method methods2;
          try {
            methods2 = c.getMethod("set" + filename, String.class);// 注意参数不是String,是string 类型
            String colval = rs.getString(filename);
            methods2.invoke(object, colval);// 通过对象，调用有参数的方法
            // 如果这个地方需要持久保存，那么就是object类放进去。不然就是加上c.newInstance()
          } catch (Exception e) {
            methods2 = c.getMethod("set" + filename, int.class);// 注意参数不是String,是string 类型
            int colval = rs.getInt(i);
            methods2.invoke(object, colval);// 通过对象，调用有参数的方法
          }

        }
        jsonArray.add(object);
      }
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    finally {
      this.close();
    }
    // System.out.print(cols.toString());

    return jsonArray;
  }



  /**
   * 返回json格式的结果返回值
   * 
   * @param <T>
   * @param sql
   * @return
   */
  public String searchnopagesqljson(String sql) {

    String text = "";// 最终要返回的json格式字符串



    JSONArray jsonArray = new JSONArray();
    try {
      con = config.dataSource.getConnection();
      con.setAutoCommit(false);
      ps = con.prepareStatement(sql);

      rs = ps.executeQuery();
      ResultSetMetaData rsmd = rs.getMetaData();
      int colnum = rsmd.getColumnCount();
      while (rs.next()) {
        JSONObject jObject = new JSONObject();
        for (int i = 1; i <= colnum; i++) {
          String colname = rsmd.getColumnName(i);
          String colval = rs.getString(i);

          jObject.put(colname, colval);
        }
        jsonArray.add(jObject);
      }
      con.commit();
    } catch (SQLException e) {
      try {
        con.rollback();
      } catch (SQLException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    finally {
      this.close();
    }
    // System.out.print(cols.toString());
    text = jsonArray.toJSONString();
    return text;


  }

  /**
   * 返回map格式的
   * 
   * @param sql
   * @return
   */
  public Object searchnopagesqlobject(String sql) {

    String text = "";// 最终要返回的json格式字符串
    try {
      con = config.dataSource.getConnection();
    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    if (con != null) {
    } else {
    }

    ArrayList jsonArray = new ArrayList();
    try {


      ps = con.prepareStatement(sql);

      rs = ps.executeQuery();
      ResultSetMetaData rsmd = rs.getMetaData();
      int colnum = rsmd.getColumnCount();
      while (rs.next()) {
        Map jObject = new HashMap();
        for (int i = 1; i <= colnum; i++) {
          String colname = rsmd.getColumnName(i);
          String colval = rs.getString(i);

          jObject.put(colname, colval);
        }
        jsonArray.add(jObject);
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      this.close();
    }

    // System.out.print(cols.toString());

    return jsonArray;
  }



  /**
   * 返回map格式的,带分页效果,基于oracle
   * 
   * @param sql，所需要查询的SQL语句
   * @param page,需要查询的页码
   * @param pagesize,需要查询的一页显示的数量
   * @return
   */
  public Object searchpagesqlobject(String sql, int page, int pagesize) {

    String text = "";// 最终要返回的json格式字符串
    try {
      con = config.dataSource.getConnection();
    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    if (con != null) {
    } else {
    }

    ArrayList jsonArray = new ArrayList();
    try {
      int pagesize1 = 0;
      int page1 = 0;
      pagesize1 = pagesize * page;
      page1 = ((page - 1) * pagesize) + 1;// 获取到其实的显示数量

      String sql1 = "select * from ( select sasasdasds.*,ROWNUM RN from (";
      sql1 += sql;
      sql1 += "  ) sasasdasds where ROWNUM<=" + pagesize1 + ") where RN>= " + page1;
      System.out.println(sql1);
      ps = con.prepareStatement(sql1);

      rs = ps.executeQuery();
      ResultSetMetaData rsmd = rs.getMetaData();
      int colnum = rsmd.getColumnCount();
      colnum = colnum - 1;
      while (rs.next()) {
        Map jObject = new HashMap();
        for (int i = 1; i <= colnum; i++) {
          String colname = rsmd.getColumnName(i);
          String colval = rs.getString(i);
          jObject.put(colname, colval);
        }
        jsonArray.add(jObject);
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      this.close();
    }

    // System.out.print(cols.toString());

    return jsonArray;
  }



  /**
   * 返回json格式的,带分页效果
   * 
   * @param sql，所需要查询的SQL语句
   * @param page,需要查询的页码
   * @param pagesize,需要查询的一页显示的数量
   * @return
   */
  public String searchpagesqljson(String sql, int page, int pagesize) {

    String text = "";// 最终要返回的json格式字符串
    try {
      con = config.dataSource.getConnection();
    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    if (con != null) {
    } else {
    }

    JSONArray jsonArray = new JSONArray();
    try {
      int pagesize1 = 0;
      int page1 = 0;
      pagesize1 = pagesize * page;
      page1 = ((page - 1) * pagesize) + 1;// 获取到其实的显示数量

      String sql1 = "select * from ( select sasasdasds.*,ROWNUM RN from (";
      sql1 += sql;
      sql1 += "  ) sasasdasds where ROWNUM<=" + pagesize1 + ") where RN>= " + page1;
      ps = con.prepareStatement(sql1);

      rs = ps.executeQuery();
      ResultSetMetaData rsmd = rs.getMetaData();
      int colnum = rsmd.getColumnCount();
      colnum = colnum - 1;
      while (rs.next()) {
        JSONObject jObject = new JSONObject();
        for (int i = 1; i <= colnum; i++) {
          String colname = rsmd.getColumnName(i);
          String colval = rs.getString(i);
          jObject.put(colname, colval);
        }
        jsonArray.add(jObject);
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      this.close();
    }

    // System.out.print(cols.toString());
    text = jsonArray.toJSONString();
    return text;
  }

  /**
   * 只获取一条中指定字段的值
   * 
   * @return
   */
  public String getnosqloneval(String sql) {
    String val = "";
    try {
      con = config.dataSource.getConnection();
      ps = con.prepareStatement(sql);

      rs = ps.executeQuery();
      if (rs.next()) {
        val = rs.getString(1);
      }
    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } finally {
      this.close();
    }
    return val;
  }

  /**
   * 只获取多条中指定字段的值
   * 
   * @return 一个身份证号码的集合
   */
  public ArrayList<String> getnosqlallval(String sql) {
    ArrayList<String> valdates = new ArrayList<String>();
    String val = "";
    try {
      con = config.dataSource.getConnection();
      ps = con.prepareStatement(sql);

      rs = ps.executeQuery();
      while (rs.next()) {
        val = rs.getString(1);
        valdates.add(val);
      }
    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } finally {
      this.close();
    }
    return valdates;
  }


  /**
   * 增,删,改表数据
   * 
   * @param sql
   * @return
   */
  public boolean executesql(String sql) {
    boolean arg = false;
    int a = 0;
    try {
      con = config.dataSource.getConnection();
      con.setAutoCommit(false);
      ps = con.prepareStatement(sql);

      a = ps.executeUpdate();
      con.commit();
    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
      // arg=true;
      try {
        con.rollback();
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        // arg=true;
      }
    } finally {
      this.close();
    }
    if (a > 0) {
      arg = true;
    }
    return arg;
  }


  /**
   * 批量增,删,改表数据
   * 
   * @param sql
   * @return
   */
  public boolean executealsql(ArrayList sql) {
    boolean arg = false;

    try {
      con = config.dataSource.getConnection();
      con.setAutoCommit(false);
      for (Object object : sql) {
        String sql1 = String.valueOf(object);
        ps = con.prepareStatement(sql1);
        arg = ps.execute();

      }
      con.commit();

    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
      // System.out.println("唯一性违反");
      try {
        con.rollback();
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } finally {
      this.close();
    }
    return arg;
  }

}
