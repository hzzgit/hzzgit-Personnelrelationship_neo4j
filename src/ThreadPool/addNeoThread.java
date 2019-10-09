package ThreadPool;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.log4j.Logger;
import tools.Node4jtool;
import util.util;
import Neo4jdao.Neo4jDriver;

/**
 * 用来批量添加结点的线程
 *
 * @author Administrator
 */
public class addNeoThread implements Runnable {
    private static Logger logger = Logger.getLogger(mainThread.class);

    private Neo4jDriver neo4jDriver = null;
    private HashSet<String> adddate = null;// 节点数据
    private ArrayList<String> linkdate = null;// 节点数据

    public addNeoThread(HashSet<String> adddate, ArrayList<String> linkdate) {

        this.adddate = adddate;
        this.linkdate = linkdate;

        // TODO Auto-generated constructor stub
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        try {
            System.out.println("进行Neo线程");
            util.addthread += 1;
            if (util.addthread <= util.toolco) {// 如果当前运行线程Id小于总线程约束
                int poll = 0;
                synchronized (util.neoqueue) {// 从neo4j连接池中获取数据
                    poll = util.neoqueue.poll();
                    System.out.println("现在运行的的是第" + poll + "个neo连接");
                    logger.debug("现在运行的的是第" + poll + "个neo连接");
                    util.neoqueue.offer(poll);
                }
                this.neo4jDriver = (Neo4jDriver) Node4jtool.nodedridate.get(poll);
            } else {
                this.neo4jDriver = (Neo4jDriver) Node4jtool.nodedridate.get(util.toolco + 1);
            }
            // 添加点
            System.out.println(Thread.currentThread().getName() + "共运行了" + (util.addthread) + "个线程");
            logger.debug(Thread.currentThread().getName() + "共运行了" + (util.addthread) + "个线程");

            long startTime = System.currentTimeMillis();// 记录开始时间
            neo4jDriver.createnodemore(adddate);
            // neo4jDriver.createnodemorepiliang(adddate);
            long endTime = System.currentTimeMillis();// 记录结束时间
            float excTime = (float) (endTime - startTime) / 1000;

            System.out.println(Thread.currentThread().getName() + "添加了：" + adddate.size() + "个节点" + "花费了"
                    + excTime + "s");
            logger.debug(Thread.currentThread().getName() + "添加了：" + adddate.size() + "个节点" + "花费了"
                    + excTime + "s");
            // 记录添加数
            util.setfinishnode(adddate.size());

            // 添加线
            startTime = System.currentTimeMillis();// 记录开始时间
            neo4jDriver.createnodemore(linkdate);
            // neo4jDriver.createnodemorepiliang(linkdate);
            endTime = System.currentTimeMillis();// 记录结束时间
            excTime = (float) (endTime - startTime) / 1000;
            System.out.println(Thread.currentThread().getName() + "添加了：" + linkdate.size() + "个线" + "花费了"
                    + excTime + "s");
            logger.debug(Thread.currentThread().getName() + "添加了：" + linkdate.size() + "个线" + "花费了"
                    + excTime + "s");
            // 记录添加数
            util.setfinishlink(linkdate.size());
            util.addthread -= 1;

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // neo4jDriver.close();
        }

    }

}
