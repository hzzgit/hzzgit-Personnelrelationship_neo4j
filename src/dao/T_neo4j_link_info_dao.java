package dao;

import java.util.ArrayList;

import daoconfig.oracledb;
import been.T_neo4j_link_info;
import been.T_neo4j_node;

/**
 * 关联表dao
 * 
 * @author Administrator
 *
 */
public class T_neo4j_link_info_dao {
	private oracledb oracledb=new oracledb();
	
	/**
	 * 返回所有关联节点表信息
	 * @return
	 */
	 public ArrayList<T_neo4j_link_info> getallnode(){
		 ArrayList<T_neo4j_link_info> nodes=new  ArrayList<T_neo4j_link_info>();
		 String sql="select * from tc_tools.t_neo4j_link_info";
		 nodes=(ArrayList<T_neo4j_link_info>) oracledb.searchnopagesqlclass(sql, new T_neo4j_link_info() );
		 return nodes;
	 }
}
