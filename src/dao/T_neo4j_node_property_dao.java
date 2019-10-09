package dao;

import java.util.ArrayList;

import been.T_neo4j_link_info;
import been.T_neo4j_node_property;
import daoconfig.oracledb;

/**
 * 节点属性类
 * @author Administrator
 *
 */
public class T_neo4j_node_property_dao {
	private oracledb oracledb=new oracledb();
	
	/**
	 * 返回所有关联节点对应属性信息,根据关联Id
	 * @return
	 */
	 public ArrayList<T_neo4j_node_property> getnodepropertybyid(String nodeid){
		 ArrayList<T_neo4j_node_property> nodes=new  ArrayList<T_neo4j_node_property>();
		 String sql="select * from tc_tools.t_neo4j_node_property where node_id ='"+nodeid+"'";
		 nodes=(ArrayList<T_neo4j_node_property>) oracledb.searchnopagesqlclass(sql, new T_neo4j_node_property() );
		 return nodes;
	 }
}
