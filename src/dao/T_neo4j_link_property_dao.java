package dao;

import java.util.ArrayList;

import been.T_neo4j_link_property;
import been.T_neo4j_node_property;
import daoconfig.oracledb;

/**
 * 关联跟随属性
 * @author Administrator
 *
 */
public class T_neo4j_link_property_dao {
	private oracledb oracledb=new oracledb();
	
	/**
	 * 获取关联属性获取
	 * @param nodeid
	 * @return
	 */
	public ArrayList<T_neo4j_link_property> getlinkpropertybyid(String nodeid){
		 ArrayList<T_neo4j_link_property> nodes=new  ArrayList<T_neo4j_link_property>();
		 String sql="select * from tc_tools.t_neo4j_link_property where link_id ='"+nodeid+"'";
		 nodes=(ArrayList<T_neo4j_link_property>) oracledb.searchnopagesqlclass(sql, new T_neo4j_link_property() );
		 return nodes;
	 }
}
