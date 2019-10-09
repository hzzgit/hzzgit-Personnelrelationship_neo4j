package tools;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;

public class properutil {


  /**
   * 修改配置文件,亲属关系的增量
   * 
   * @param zlval
   */
  public static void updateconfig(String zlval) {
    Properties properties = new Properties();
    try {
      FileOutputStream file = new FileOutputStream("otherzl.properties");
      System.out.println("修改增量" + zlval);
      properties.setProperty("qszl", zlval);
      properties.store(file, "数据库配置修改"); // 这句话表示重新写入配置文件
    } catch (IOException e) {
      // TODO 自动生成的 catch 块
      e.printStackTrace();
    }
  }



  /**
   * 获取亲属关系的增量
   */
  @Test
  public static String getqsval() {
    String time = "";
    try {
      Properties properties = new Properties();
      properties.load(new FileInputStream("otherzl.properties"));
      time = properties.getProperty("qszl");
      System.out.println(time);
    } catch (IOException e) {
      // TODO 自动生成的 catch 块
      e.printStackTrace();
    }
    return time;
  }


  /**
   * 获取线程每次运行的时间
   */
  public static String gettimeproperties() {
    String time = "";


    try {
      Properties properties = new Properties();
      properties.load(new FileInputStream("time.properties"));
      time = properties.getProperty("time");
    } catch (IOException e) {
      // TODO 自动生成的 catch 块
      e.printStackTrace();
    }
    return time;
  }
}
