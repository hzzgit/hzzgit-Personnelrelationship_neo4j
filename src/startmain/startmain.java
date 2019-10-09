package startmain;

import tools.Node4jtool;
import util.util;
import Controller.ReadController;
import Neo4jdao.Neo4jDriver;
import daoconfig.gbasemysqlstart;

public class startmain {


  public static void main(String[] args) {

    gbasemysqlstart gbasemysqlstart = new gbasemysqlstart("");
    // Myframe.getMyFrame();//启动界面
    // Myframe.myframe.jf.setText("开始启动");
    ReadController readControlle = new ReadController();
    util.setpagesize();// 改变页数，创建插入neo4j线程池，从配置文件中获取
    for (int i = 1; i <= util.toolco + 1; i++) {// 先生成neo驱动池,与添加线程池数量一致
      // 192.168.16.241
      Node4jtool.nodedridate.put(i, new Neo4jDriver("bolt://127.0.0.1:7687", "neo4j", "tch123456"));
      if (i != util.toolco + 1) {
        util.neoqueue.offer(i);
      }
    }


    readControlle.readCreateThreadPool();


  }
}
