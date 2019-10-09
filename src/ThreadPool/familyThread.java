package ThreadPool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import org.apache.log4j.Logger;
import tools.Node4jtool;
import tools.properutil;
import util.util;
import Controller.ReadController;
import Neo4jdao.Neo4jDriver;
import beanpojo.V_analysis_jtgx;
import dao.family_dao;
import dao.sysdao;
import familySearchThread.familySearchThread;

/**
 * 亲属关系的线程
 * 
 * @author Administrator
 * 
 */
public class familyThread implements Runnable {
  private static Logger logger = Logger.getLogger(familyThread.class);

  private family_dao family_dao = new family_dao(); // 亲属的dao
  private sysdao sysdao = new sysdao();
  private String zlval = ""; // 亲属关系的增量

  private String endval = ""; // 结束值增量
  private int allcount = 0; // 总数。用于分页

  @Override
  public void run() {
    try {
      System.out.println("进行亲属线程");
      // TODO Auto-generated method stub
      zlval = properutil.getqsval();// 获取到亲属关系的增量

      endval = sysdao.getstarttablecount("WHEN_OPERATED", "2", "tc_tools", "v_analysis_jtgx");// 获取增量最大值

      Thread.sleep(1500);
      if ("0".equals(zlval)) {// 全量运行
        allcount =
            family_dao.getstarttablecount("tc_tools", "v_analysis_jtgx", "WHEN_OPERATED", "2", "",
                endval);
      } else {
        allcount =
            family_dao.getstarttablecount("tc_tools", "v_analysis_jtgx", "WHEN_OPERATED", "2",
                zlval, endval);
      }
      System.out.println("总共有" + allcount);
      int fenco = allcount / util.pagesize;
      int yuco = allcount % util.pagesize;
      if (yuco > 0) {// 如果余数大于0那么分页的页码要加1
        fenco = fenco + 1;
      }
      HashSet<String> allnodedates = new HashSet<String>();// 节点集合
      ArrayList<String> linksdates = new ArrayList<String>();// 关联线集合

      for (int i = 1; i <= fenco; i++) {// 进行分页查询
        System.out.println("当前" + i + "页，共" + fenco + "页");
        logger.debug("当前" + i + "页，共" + fenco + "页");
        ArrayList jtgxs = new ArrayList();// 起始表亲属关系
        String cqtype = "全量";
        if ("".equals(zlval) || "0".equals(zlval) || zlval == null) {// 如果增量字段为空或者是0，全量抽取
          jtgxs =
              family_dao.getjudgedates(i, util.pagesize, "tc_tools", "v_analysis_jtgx",
                  "WHEN_OPERATED", "2", "", endval);
          cqtype = "全量";
          Thread thread = new Thread(new familySearchThread(jtgxs, cqtype));// 将亲属关系抽取拼接的线程独立出来
          ReadController.executorsearchServices.submit(thread);// 添加进去
        } else {// 增量抽取
          jtgxs =
              family_dao.getjudgedates(i, util.pagesize, "tc_tools", "v_analysis_jtgx",
                  "WHEN_OPERATED", "2", zlval, endval);
          cqtype = "增量";
          biduidates(jtgxs, cqtype);// 进行生成节点和关系线
        }
        // System.out.println(jtgxs.size());
        try {

        } catch (Exception e) {
          e.printStackTrace();
          logger.error(e);
        }
      }

      // 这里是修改增量
      String endzlval = endval.substring(0, endval.length() - 2);
      properutil.updateconfig(endzlval);// 修改增量值
      logger.error("亲属此次增量值为" + endval);
    } catch (Exception e) {
      logger.error(e);
    }
  }

  /**
   * 进行节点和关系的获取
   * 
   * @param jtgxs,所有当前页要进行分析的家庭关系，
   * @param cqtype ,分为全量和增量，如果是全量抽取 则不需要去判断这个身份证是否存在，直接插入，如果是增量，那么就判断这歌身份证是否存在吗，存在就删除并重新计算
   */
  private void biduidates(ArrayList jtgxs, String cqtype) throws Exception {
    long startTime = System.currentTimeMillis();// 记录开始时间
    HashSet<String> allnodedates = new HashSet<String>();// 节点保存集合
    ArrayList<String> linksdates = new ArrayList<String>();// 关联线集合
    int staco = 0;
    for (int i = 0; i < jtgxs.size(); i++) {// 根据户籍Id进行遍历
      Map<String, String> date = (Map<String, String>) jtgxs.get(i);
      String hujiid = date.get("HU_ID_NEW");// 获取到户籍id
      // System.out.println(hujiid);
      ArrayList<V_analysis_jtgx> hudates = family_dao.checkhuji(hujiid);// 获取整个户籍的情况
      String nodestring = "";// 节点插入语句
      V_analysis_jtgx huzhudate = new V_analysis_jtgx();// 户主节点
      if ("增量".equals(cqtype)) {// 如果是增量，那么要去遍历这个户籍的人是否存在，存在就删除
        for (V_analysis_jtgx jtgx : hudates) {// 遍历整个户籍，生成节点，并获取户主id
          Neo4jDriver driver = (Neo4jDriver) Node4jtool.nodedridate.get(util.toolco + 1);// 获取到额外存在的连接
          boolean delarg =
              driver.checknodes("match (p1:Persons) where p1.sfzh='" + jtgx.getPid()
                  + "' return p1");// 根据身份证查询是否已经存在，如果已经存在，那么就进行删除操作
          if (delarg) {
            String cql =
                "match (p1:Persons) where p1.sfzh='" + jtgx.getPid()
                    + "'  match (p2:Persons)  match p= (p1)-[r:qsgx]-(p2)  return p";
            Map<String, HashSet<Map<String, Object>>> test = driver.printJSON(cql);
            HashSet<Map<String, Object>> nodes = test.get("nodes");
            driver.createnode("match (p1:Persons) where p1.sfzh='" + jtgx.getPid()
                + "' match (p2:Persons)  match p= (p1)<-[r:qsgx]->(p2) delete r");// 这个可以不理解成创建节点。而是执行删除语句
          }
        }
      }
      for (V_analysis_jtgx jtgx : hudates) {// 遍历整个户籍，生成节点，并获取户主id
        staco += hudates.size();// 计算遍历多少条
        String pid = jtgx.getPid();// 开始节点本身的身份证
        String name = jtgx.getName();// 开始节点本身的姓名
        String f_pid = jtgx.getFa_pid();// 父亲节点的身份证号
        String f_name = jtgx.getFa_name();// 父亲节点的姓名
        String ma_pid = jtgx.getMa_pid();// 母亲节点的身份证号
        String ma_name = jtgx.getMa_name();// 母亲节点的姓名
        String po_id = jtgx.getPo_pid();// 配偶的身份证号
        String po_name = jtgx.getPo_name();// 配偶的姓名
        String guanrdian1_pid = jtgx.getGurardian_1_pid();// 监护人1的身份证号
        String guanrdian1_name = jtgx.getGurardian_1();// 监护人1的姓名
        String guanrdian2_pid = jtgx.getGurardian_2_pid();// 监护人2的身份证
        String guanrdian2_name = jtgx.getGurardian_2();// 监护人2的姓名
        String native_place = jtgx.getNative_place();// 居住地
        String nodedate = "";// 节点添加语句
        String linkdate = "";// 关系线添加语句
        // ArrayList<V_analysis_jtgx> zngx = family_dao.checkzinv(pid);// 查询出子女关系

        if (f_pid != null && !"".equals(f_pid)) {// 父亲节点不为空
          if (f_name != null && !"".equals(f_name)) {// 父亲节点不为空
            int co = family_dao.checkqscount("fa_pid", f_pid);
            if (co <= 15) {// 必须小于15个，不然就是集体户
              String sex = family_dao.checkxbbyid(f_pid);
              nodedate =
                  Node4jtool.getnodecql("Persons", "Persons", "sfzh:'" + f_pid + "',xm:'" + f_name
                      + "',xb:'" + sex + "'");
              allnodedates.add(nodedate);
              String shipdate = "address:'" + native_place + "',gx:'父亲',gz:'qsgx'";
              linkdate =
                  Node4jtool.getrelationship("sfzh", "sfzh", "Persons", "Persons", pid, f_pid,
                      "qsgx", shipdate, "2");
              linksdates.add(linkdate);
            }
          }
        }
        if (ma_pid != null && !"".equals(ma_pid)) {// 母亲节点不为空
          if (ma_name != null && !"".equals(ma_name)) {// 母亲节点不为空
            int co = family_dao.checkqscount("ma_pid", ma_pid);
            if (co <= 15) {// 必须小于15个，不然就是集体户
              String sex = family_dao.checkxbbyid(ma_pid);
              nodedate =
                  Node4jtool.getnodecql("Persons", "Persons", "sfzh:'" + ma_pid + "',xm:'"
                      + ma_name + "',xb:'" + sex + "'");
              allnodedates.add(nodedate);
              String shipdate = "address:'" + native_place + "',gx:'母亲',gz:'qsgx'";
              linkdate =
                  Node4jtool.getrelationship("sfzh", "sfzh", "Persons", "Persons", pid, ma_pid,
                      "qsgx", shipdate, "2");
              linksdates.add(linkdate);
            }

          }
        }
        if (po_id != null && !"".equals(po_id)) {// 配偶节点不为空
          if (po_name != null && !"".equals(po_name)) {// 配偶节点不为空
            int co = family_dao.checkqscount("po_pid", po_id);
            if (co <= 15) {// 必须小于15个，不然就是集体户
              String sex = family_dao.checkxbbyid(po_id);
              nodedate =
                  Node4jtool.getnodecql("Persons", "Persons", "sfzh:'" + po_id + "',xm:'" + po_name
                      + "',xb:'" + sex + "'");
              allnodedates.add(nodedate);
              String shipdate = "address:'" + native_place + "',gx:'配偶',gz:'qsgx'";
              linkdate =
                  Node4jtool.getrelationship("sfzh", "sfzh", "Persons", "Persons", pid, po_id,
                      "qsgx", shipdate, "2");
              linksdates.add(linkdate);
            }
          }
        }

        if (guanrdian1_pid != null && !"".equals(guanrdian1_pid)) {// 监护人节点不为空
          if (guanrdian1_name != null && !"".equals(guanrdian1_name)) {// 监护人节点不为空
            if (!guanrdian1_pid.equals(f_pid) && !guanrdian1_pid.equals(ma_pid)) {// 监护人id不等于父亲和母亲
              int co = family_dao.checkqscount("GURARDIAN_1_PID", guanrdian1_pid);
              if (co <= 15) {// 必须小于15个，不然就是集体户
                String sex = family_dao.checkxbbyid(guanrdian1_pid);
                nodedate =
                    Node4jtool.getnodecql("Persons", "Persons", "sfzh:'" + guanrdian1_pid
                        + "',xm:'" + guanrdian1_name + "',xb:'" + sex + "'");
                allnodedates.add(nodedate);
                String shipdate = "address:'" + native_place + "',gx:'监护人1',gz:'qsgx'";
                linkdate =
                    Node4jtool.getrelationship("sfzh", "sfzh", "Persons", "Persons", pid,
                        guanrdian1_pid, "qsgx", shipdate, "2");
                linksdates.add(linkdate);
              }
            }
          }
        }

        if (guanrdian2_pid != null && !"".equals(guanrdian2_pid)) {// 监护人2节点不为空
          if (guanrdian2_name != null && !"".equals(guanrdian2_name)) {// 监护人2节点不为空
            if (!guanrdian2_pid.equals(f_pid) && !guanrdian2_pid.equals(ma_pid)) {// 监护人2id不等于父亲和母亲
              int co = family_dao.checkqscount("GURARDIAN_2_PID", guanrdian1_pid);
              if (co <= 15) {// 必须小于15个，不然就是集体户
                String sex = family_dao.checkxbbyid(guanrdian2_pid);
                nodedate =
                    Node4jtool.getnodecql("Persons", "Persons", "sfzh:'" + guanrdian2_pid
                        + "',xm:'" + guanrdian2_name + "',xb:'" + sex + "'");
                allnodedates.add(nodedate);
                String shipdate = "address:'" + native_place + "',gx:'监护人2',gz:'qsgx'";
                linkdate =
                    Node4jtool.getrelationship("sfzh", "sfzh", "Persons", "Persons", pid,
                        guanrdian2_pid, "qsgx", shipdate, "2");
                linksdates.add(linkdate);
              }
            }
          }
        }

        // for (V_analysis_jtgx zngx1 : zngx) {// 子女关系
        // String sex = zngx1.getGender();// 获取到性别
        // nodedate =
        // Node4jtool.getnodecql("Persons", "Persons", "sfzh:'" + zngx1.getPid() + "',xm:'"
        // + zngx1.getName() + "',xb:'" + sex + "'");
        // allnodedates.add(nodedate);
        // String shipdate = "address:'" + native_place + "',gx:'子女',gz:'qsgx'";
        // linkdate =
        // Node4jtool.getrelationship("sfzh", "sfzh", "Persons", "Persons", zngx1.getPid(), pid,
        // "qsgx", shipdate, "1");
        // linksdates.add(linkdate);
        // }

        nodestring =
            Node4jtool.getnodecql("Persons", "Persons",
                "sfzh:'" + jtgx.getPid() + "',xm:'" + jtgx.getName() + "',xb:'" + jtgx.getGender()
                    + "'");
        allnodedates.add(nodestring);
        String master_relation = jtgx.getMaster_relation();// 户籍类型
        if ("户主".equals(master_relation)) {// 如果是户主
          huzhudate = jtgx;// 户主节点保存
        }
      }

      for (V_analysis_jtgx hudate : hudates) {// 遍历整个户籍 ,获取到与户主的关系线
        String hupid = huzhudate.getPid();// 户主pid
        String linkdate = "";
        String master_relation = hudate.getMaster_relation();// 户籍类型
        String native_place = hudate.getNative_place();// 地址
        if (!"户主".equals(master_relation)) {// 不是户主的情况
          if ("子".equals(master_relation.trim())) {
            master_relation = "儿子";
          }
          if ("女".equals(master_relation.trim())) {
            master_relation = "女儿";
          }
          if ("妻".equals(master_relation.trim())) {
            master_relation = "妻子";
          }
          String shipdate = "address:'" + native_place + "',gx:'" + master_relation + "',gz:'qsgx'";
          linkdate =
              Node4jtool.getrelationship("sfzh", "sfzh", "Persons", "Persons", hudate.getPid(),
                  hupid, "qsgx", shipdate, "1");
          linksdates.add(linkdate);
        }
      }

    }

    // for (String string : linksdates) {
    // System.out.println(string);
    // }
    // for (String string : allnodedates) {
    // System.out.println(string);
    // }

    long endTime = System.currentTimeMillis();// 记录结束时间
    float excTime = (float) (endTime - startTime) / 1000;
    Thread thread = new Thread(new addNeoThread(allnodedates, linksdates));// 批量添加结点数据的线程
    ReadController.executorService2.submit(thread);
    System.out.println("亲属开始节点的数量" + staco);
    System.out.println("亲属要进行添加的节点数" + allnodedates.size());
    System.out.println("亲属要进行添加的关联数" + linksdates.size());
    System.out.println("执行时间：" + excTime + "s");
    logger.debug("亲属开始节点的数量" + staco);
    logger.debug("亲属要进行添加的节点数" + allnodedates.size());
    logger.debug("亲属要进行添加的关联数" + linksdates.size());
    logger.debug("执行时间：" + excTime + "s");
    // 这里是计算总数
    util.setchulidate(staco);
    util.setneedaddnode(allnodedates.size());
    util.setneedaddlink(linksdates.size());
  }
  /**
   * 进行户籍人员信息的拼接
   */
  // private void biduidates( ArrayList<V_analysis_jtgx> jtgxs){
  // HashSet<String> allnodedates=new HashSet<String>();//节点集合
  // ArrayList<String> linksdates=new ArrayList<String>();//关联线集合
  // for (V_analysis_jtgx jtgx : jtgxs) {
  // String pid=jtgx.getPid();//开始节点本身的身份证
  // String name=jtgx.getName();//开始节点本身的姓名
  // String f_pid=jtgx.getFa_pid();//父亲节点的身份证号
  // String f_name=jtgx.getFa_name();//父亲节点的姓名
  // String ma_pid=jtgx.getMa_pid();//母亲节点的身份证号
  // String ma_name=jtgx.getMa_name();//母亲节点的姓名
  // String po_id=jtgx.getPo_pid();//配偶的身份证号
  // String po_name=jtgx.getPo_name();//配偶的姓名
  // String guanrdian1_pid=jtgx.getGurardian_1_pid();//监护人1的身份证号
  // String guanrdian1_name=jtgx.getGurardian_1();//监护人1的姓名
  // String guanrdian2_pid=jtgx.getGurardian_2_pid();//监护人2的身份证
  // String guanrdian2_name=jtgx.getGurardian_2();//监护人2的姓名
  // String native_place=jtgx.getNative_place();//居住地
  // String nodedate="";//节点添加语句
  // String linkdate="";//关系线添加语句
  // ArrayList<V_analysis_jtgx> zngx=family_dao.checkzinv(pid);//查询出子女关系
  // if(f_pid!=null&&!"".equals(f_pid)){//父亲节点不为空
  // if(f_name!=null&&!"".equals(f_name)){//父亲节点不为空
  // nodedate=Node4jtool.getnodecql("Persons", "Persons",
  // "sfzh:'"+f_pid+"',xm:'"+f_name+"'");
  // allnodedates.add(nodedate);
  // String shipdate="address:'"+native_place+",gz:'父亲'";
  // linkdate=Node4jtool.getrelationship("sfzh", "sfzh", "Persons", "Persons",
  // pid, f_pid, "qsgx", shipdate, "2");
  // }
  // }
  // if(ma_pid!=null&&!"".equals(ma_pid)){//母亲节点不为空
  // if(ma_name!=null&&!"".equals(ma_name)){//母亲节点不为空
  // nodedate=Node4jtool.getnodecql("Persons", "Persons",
  // "sfzh:'"+ma_pid+"',xm:'"+ma_name+"'");
  // allnodedates.add(nodedate);
  // }
  // }
  // if(po_id!=null&&!"".equals(po_id)){//配偶节点不为空
  // if(po_name!=null&&!"".equals(po_name)){//配偶节点不为空
  // nodedate=Node4jtool.getnodecql("Persons", "Persons",
  // "sfzh:'"+po_id+"',xm:'"+po_name+"'");
  // allnodedates.add(nodedate);
  // }
  // }
  // if(guanrdian1_pid!=null&&!"".equals(guanrdian1_pid)){//监护人节点不为空
  // if(guanrdian1_name!=null&&!"".equals(guanrdian1_name)){//监护人节点不为空
  // if(!guanrdian1_pid.equals(f_pid)&&!!guanrdian1_pid.equals(ma_pid)){//监护人id不等于父亲和母亲
  // nodedate=Node4jtool.getnodecql("Persons", "Persons",
  // "sfzh:'"+guanrdian1_pid+"',xm:'"+guanrdian1_name+"'");
  // allnodedates.add(nodedate);
  // }
  // }
  // }
  //
  // if(guanrdian2_pid!=null&&!"".equals(guanrdian2_pid)){//监护人2节点不为空
  // if(guanrdian2_name!=null&&!"".equals(guanrdian2_name)){//监护人2节点不为空
  // if(!guanrdian2_pid.equals(f_pid)&&!!guanrdian2_pid.equals(ma_pid)){//监护人2id不等于父亲和母亲
  // nodedate=Node4jtool.getnodecql("Persons", "Persons",
  // "sfzh:'"+guanrdian2_pid+"',xm:'"+guanrdian2_name+"'");
  // allnodedates.add(nodedate);
  // }
  // }
  // }
  // for (V_analysis_jtgx zngx1 : zngx) {//子女关系
  //
  // }
  //
  //
  //
  // }
  // }

}
