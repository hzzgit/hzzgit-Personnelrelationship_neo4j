package Controller;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import tools.Node4jtool;
import util.util;
import Neo4jdao.Neo4jDriver;
import ThreadPool.familyThread;
import ThreadPool.mainThread;
import beanpojo.t_neo4j_node_info_node;
import been.T_neo4j_link_info;
import been.T_neo4j_node_property;
import dao.T_neo4j_link_info_dao;
import dao.T_neo4j_node_property_dao;
import dao.t_neo4j_node_info_node_dao;

/**
 * 控制层
 * 
 * @author Administrator
 * 
 */
public class ReadController {

  private static Logger logger = Logger.getLogger(mainThread.class);

  // neo公共线程池
  public static ExecutorService executorService2 = null;
  // 用来全量抽取的时候的公共线程池
  public static ExecutorService executorsearchServices = null;
  // 关联信息查询
  private T_neo4j_link_info_dao t_neo4j_link_info_dao = new T_neo4j_link_info_dao();
  // 节点关联总结点
  private t_neo4j_node_info_node_dao t_neo4j_node_info_node_dao = new t_neo4j_node_info_node_dao();
  // 节点属性查询
  private T_neo4j_node_property_dao t_neo4j_node_property_dao = new T_neo4j_node_property_dao();
  // 计时器
  private ScheduledExecutorService mScheduledExecutorService = Executors.newScheduledThreadPool(4);

  /**
   * 读取配置表，并创建线程池
   */

  public void readCreateThreadPool() {

    mScheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        try {
          ReadController.executorService2 = Executors.newFixedThreadPool(util.toolco);
          ReadController.executorsearchServices = Executors.newFixedThreadPool(util.toolco);
          // 创建neo插入线程池和查询遍历生成的线程池
          long startTime = System.currentTimeMillis();// 记录开始时间
          ArrayList<T_neo4j_link_info> infos = new ArrayList<T_neo4j_link_info>();
          infos = t_neo4j_link_info_dao.getallnode();
          ExecutorService executorService = Executors.newFixedThreadPool(12);

          createnodeyuesu();// 创建唯一性约束

          int driverco = 1;// 驱动读取Id，已经无用
          // 先添加亲属
          familyThread familyThread = new familyThread();
          executorService.submit(familyThread);// 线程池添加线程,亲属关系
          // 每个线程都是有带关键字和OID字段
          for (T_neo4j_link_info t_neo4j_link_info : infos) {
            // T_neo4j_link_info t_neo4j_link_info=new
            // T_neo4j_link_info();
            // t_neo4j_link_info=infos.get(0);
            String startnode_id = t_neo4j_link_info.getStart_node();// 开始节点
            String endnode_id = t_neo4j_link_info.getEnd_node();// 结束节点
            String linkname = t_neo4j_link_info.getLink_name();// 关系名
            String aspect_type = t_neo4j_link_info.getAspect_type();// 单向OR双向
            t_neo4j_node_info_node startinfonode = new t_neo4j_node_info_node();// 开始节点
            t_neo4j_node_info_node endinfonode = new t_neo4j_node_info_node();// 结束节点
            startinfonode = t_neo4j_node_info_node_dao.getallnode(startnode_id).get(0);
            endinfonode = t_neo4j_node_info_node_dao.getallnode(endnode_id).get(0);
            mainThread mainThread1 =
                new mainThread(driverco, t_neo4j_link_info.getId(), startinfonode, endinfonode,
                    linkname, aspect_type);

            executorService.submit(mainThread1);// 线程池添加线程

          }

          executorService.shutdown();
          boolean loop = false;
          while (!loop) {
            try {
              loop = executorService.awaitTermination(0, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
              logger.error(e);
            }
          }
          logger.error("第" + util.needaddnode + "轮结束" + "解析的数据数为"
              + util.chulidate.get(util.chulico) + "个");
          logger.error("第" + util.needaddnode + "轮结束" + "需要添加的节点数为"
              + util.needaddnodedate.get(util.needaddnode) + "个");
          logger.error("第" + util.needaddlink + "轮结束" + "需要添加的连接线数为"
              + util.needaddlinkdate.get(util.needaddlink) + "个");


          // 读取多线程池轮询停止
          ReadController.executorsearchServices.shutdown();
          loop = false;
          while (!loop) {
            try {
              loop = ReadController.executorsearchServices.awaitTermination(0, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
              logger.error(e);
            }
          }



          // 轮询等待添加线程结束才可以进行下一次
          ReadController.executorService2.shutdown();
          loop = false;
          while (!loop) {
            try {
              loop = ReadController.executorService2.awaitTermination(0, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
              logger.error(e);
            }
          }



          // 进入这个地方必须所有的线程池都运行结束
          System.out.println("结束");
          long endTime = System.currentTimeMillis();// 记录结束时间
          float excTime = (float) (endTime - startTime) / 1000;
          System.out.println("执行时间：" + excTime + "s");

          logger.error("此轮结束,执行时间：" + excTime + "s");

          util.needaddnode = util.needaddnode + 1;// 将存储的map添加一个
          util.needaddlink = util.needaddlink + 1;// 将存储的map添加一个
          util.finishnode = util.finishnode + 1;
          util.finishlink = util.finishlink + 1;
          util.chulico = util.chulico + 1;
          util.clearhisdate();// 清理历史数据

          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.error(e);
          }
          createindex();// 等到全量结束再创建索引
          // util.neoqueue = new LinkedList<Integer>();// 清空线程池队列

        } catch (Exception e) {
          e.printStackTrace();
          logger.error(e);
        }
      }
    }, 1, 20, TimeUnit.SECONDS);

  }

  /**
   * 创建唯一性
   */
  public void createnodeyuesu() {

    ArrayList<t_neo4j_node_info_node> t_neo4j_node_info_nodes =
        t_neo4j_node_info_node_dao.getallnode("");
    ArrayList<String> cqldate = new ArrayList<String>();
    for (t_neo4j_node_info_node t_neo4j_node_info_node : t_neo4j_node_info_nodes) {
      String lablename = t_neo4j_node_info_node.getLable_name();
      String unifield = t_neo4j_node_info_node.getUnique_field();
      String cql = Node4jtool.getcreateunique(lablename, unifield);
      cqldate.add(cql);
      ArrayList<T_neo4j_node_property> t_neo4j_node_properties =
          t_neo4j_node_property_dao.getnodepropertybyid(t_neo4j_node_info_node.getId());
      for (T_neo4j_node_property t_neo4j_node_property : t_neo4j_node_properties) {
        String isok_allow = t_neo4j_node_property.getIsok_allow();// 是否索引
        String neoname = t_neo4j_node_property.getNeo4j_name();// 字段名
        String cql1 = "";
        // if("1".equals(isok_allow)){//如果要创建索引
        // cql1=Node4jtool.getcreateindex(lablename, neoname);
        // cqldate.add(cql1);
        // }

      }
    }
    for (String string : cqldate) {
      System.out.println(string);
    }
    Neo4jDriver Neo4jDriver1 = (Neo4jDriver) Node4jtool.nodedridate.get(1);
    Neo4jDriver1.createnodemore(cqldate);// 生成索引和唯一

  }

  /**
   * 创建索引
   */
  public void createindex() {

    ArrayList<t_neo4j_node_info_node> t_neo4j_node_info_nodes =
        t_neo4j_node_info_node_dao.getallnode("");
    ArrayList<String> cqldate = new ArrayList<String>();
    for (t_neo4j_node_info_node t_neo4j_node_info_node : t_neo4j_node_info_nodes) {
      String lablename = t_neo4j_node_info_node.getLable_name();
      String unifield = t_neo4j_node_info_node.getUnique_field();
      // String cql=Node4jtool.getcreateunique(lablename, unifield);
      // cqldate.add(cql);
      ArrayList<T_neo4j_node_property> t_neo4j_node_properties =
          t_neo4j_node_property_dao.getnodepropertybyid(t_neo4j_node_info_node.getId());
      for (T_neo4j_node_property t_neo4j_node_property : t_neo4j_node_properties) {
        String isok_allow = t_neo4j_node_property.getIsok_allow();// 是否索引
        String neoname = t_neo4j_node_property.getNeo4j_name();// 字段名
        String cql1 = "";
        if ("1".equals(isok_allow)) {// 如果要创建索引
          cql1 = Node4jtool.getcreateindex(lablename, neoname);
          cqldate.add(cql1);
        }

      }
    }
    for (String string : cqldate) {
      System.out.println(string);
    }
    // Neo4jDriver Neo4jDriver1=(Neo4jDriver) Node4jtool.nodedridate.get(1);
    // Neo4jDriver1.createnodemore(cqldate);//生成索引和唯一
    //
  }
}
