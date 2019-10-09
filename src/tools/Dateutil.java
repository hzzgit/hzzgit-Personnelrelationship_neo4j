package tools;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.junit.Test;

/**
 * 时间工具类
 * 
 * @author Administrator
 * 
 */
public class Dateutil {
  /**
   * 获取当前时间
   * 
   * @return
   */
  @Test
  public static String getnowdate() {
    Date day = new Date();
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String timeString = df.format(day);

    return timeString;
  }

}
