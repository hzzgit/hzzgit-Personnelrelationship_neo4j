package util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import org.apache.log4j.Logger;

/**
 * 专门用来存放静态变量和最终变量
 * 
 * @author Administrator
 * 
 */
public class util {

  public static Queue<Integer> neoqueue = new LinkedList<Integer>();

  private static Logger logger = Logger.getLogger(util.class);

  public static int addthread = 0;// 用来记录添加线程运行的数量

  public static int pagesize = 10000;// 分页查询每页的数量
  public static int nodeco = 1;// 批量添加的Neo次数

  public static int toolco = 0;// 添加线程的数量

  public static Map<Integer, Integer> chulidate = new HashMap<Integer, Integer>();
  public static Integer chulico = 0;// 需要处理的数据量，计数

  public static Map<Integer, Integer> needaddnodedate = new HashMap<Integer, Integer>();
  public static Integer needaddnode = 0;// 需要添加的节点数,计数
  public static Map<Integer, Integer> needaddlinkdate = new HashMap<Integer, Integer>();
  public static Integer needaddlink = 0;// 需要添加的连接线数,计数


  public static Map<Integer, Integer> finishnodedate = new HashMap<Integer, Integer>();
  public static Integer finishnode = 0;// 已完成添加的节点数,计数
  public static Map<Integer, Integer> finishlinkdate = new HashMap<Integer, Integer>();
  public static Integer finishlink = 0;// 已完成添加的连接线数,计数


  public static void main(String[] args) {

  }

  /**
   * 清理历史数据
   */
  public static void clearhisdate() throws Exception {
    if (util.needaddnode > 50) {// 超过50则清0
      util.chulico = 0;
      util.needaddnode = 0;
      util.needaddlink = 0;
      util.finishnode = 0;
      util.finishlink = 0;
    }
  }

  /**
   * 记录这一轮总的处理数
   */
  public static void setchulidate(int count) throws Exception {
    Integer chuliaddco = util.chulidate.get(util.chulico);
    if (chuliaddco == null) {
      chuliaddco = 0;
    }
    chuliaddco = chuliaddco + count;
    util.chulidate.put(util.chulico, chuliaddco);
  }

  /**
   * 记录这一轮需要添加的节点数
   */
  public static void setneedaddnode(int count) throws Exception {
    Integer nodeaddco = util.needaddnodedate.get(util.needaddnode);
    if (nodeaddco == null) {
      nodeaddco = 0;
    }
    nodeaddco = nodeaddco + count;
    util.needaddnodedate.put(util.needaddnode, nodeaddco);
  }

  /**
   * 记录这一轮需要添加的线数
   * 
   * @param count
   */
  public static void setneedaddlink(int count) throws Exception {
    Integer linkaddco = util.needaddlinkdate.get(util.needaddlink);
    if (linkaddco == null) {
      linkaddco = 0;
    }
    linkaddco = linkaddco + count;
    util.needaddlinkdate.put(util.needaddlink, linkaddco);
  }

  /**
   * 记录这一轮已完成的节点
   * 
   * @param count
   */
  public static void setfinishnode(int count) throws Exception {
    Integer nodefinishco = util.finishnodedate.get(util.finishnode);
    if (nodefinishco == null) {
      nodefinishco = 0;
    }
    nodefinishco = nodefinishco + count;
    util.finishnodedate.put(util.finishnode, nodefinishco);
    logger.error("第" + util.finishnode + "轮总共添加了" + util.finishnodedate.get(util.finishnode)
        + "个节点");
    // 假如添加的和需要添加的一样多,则加1
    // if(util.finishnodedate.get(util.finishnode)==util.needaddnodedate.get(util.finishnode)){
    // logger.error("第"+util.finishnode+"轮结束，总共添加了"+util.finishnodedate.get(util.finishnode)+"个节点");
    // util.finishnode=util.finishnode+1;
    // }
  }


  /**
   * 记录这一轮已完成的线
   * 
   * @param count
   */
  public static void setfinishlink(int count) throws Exception {
    Integer linkfinishco = util.finishlinkdate.get(util.finishlink);
    if (linkfinishco == null) {
      linkfinishco = 0;
    }
    linkfinishco = linkfinishco + count;
    util.finishlinkdate.put(util.finishlink, linkfinishco);
    logger
        .error("第" + util.finishlink + "轮总共添加了" + util.finishlinkdate.get(util.finishlink) + "个线");

    // 假如添加的和需要添加的一样多,则加1
    // if(util.finishlinkdate.get(util.finishlink)==util.needaddlinkdate.get(util.finishlink)){
    // logger.error("第"+util.finishlink+"轮结束，总共添加了"+util.finishlinkdate.get(util.finishlink)+"个线");
    // util.finishlink=util.finishlink+1;
    // }
  }


  /**
   * 读取配置文件并赋值
   */
  public static void setpagesize() {
    Properties propertie = new Properties();
    FileInputStream inputFile;
    try {
      inputFile = new FileInputStream("pagesize.properties");
      propertie.load(inputFile);
      inputFile.close();
      String value = propertie.getProperty("pagesize");// 得到某一属性的值
      String toolco = propertie.getProperty("toolco");// 得到某一属性的值
      int toosize = Integer.parseInt(toolco);
      util.toolco = toosize;
      util.pagesize = Integer.parseInt(value);// 赋值


    } catch (FileNotFoundException ex) {
      System.out.println("读取属性文件--->失败！- 原因：文件路径错误或者文件不存在");
      ex.printStackTrace();
    } catch (IOException ex) {
      System.out.println("装载文件--->失败!");
      ex.printStackTrace();
    }
  }
}
