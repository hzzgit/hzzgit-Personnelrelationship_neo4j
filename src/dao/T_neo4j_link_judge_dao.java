package dao;

import java.util.ArrayList;

import been.T_neo4j_link_info;
import been.T_neo4j_link_judge;
import daoconfig.oracledb;

/**
 * 关联属性比对条件dao
 * @author Administrator
 *
 */
public class T_neo4j_link_judge_dao {
	private oracledb oracledb=new oracledb();
	
	
	/**
	 * 返回单一关系的过滤条件
	 * @return
	 */
	 public ArrayList<T_neo4j_link_judge> getallnode(String linkid){
		 ArrayList<T_neo4j_link_judge> nodes=new  ArrayList<T_neo4j_link_judge>();
		 String sql="select * from tc_tools.t_neo4j_link_judge where link_id='"+linkid+"'";
		 nodes=(ArrayList<T_neo4j_link_judge>) oracledb.searchnopagesqlclass(sql, new T_neo4j_link_judge() );
		 return nodes;
	 }
}
