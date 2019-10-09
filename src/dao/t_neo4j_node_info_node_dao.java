package dao;

import java.util.ArrayList;

import beanpojo.t_neo4j_node_info_node;
import daoconfig.oracledb;

/**
 * 节点关联dao
 * 
 * @author Administrator
 * 
 */
public class t_neo4j_node_info_node_dao {

	private oracledb	oracledb	= new oracledb();

	/**
	 * 返回所有结点的详细信息,根据节点id
	 * 
	 * @return
	 */
	public ArrayList<t_neo4j_node_info_node> getallnode(String node_Id) {
		ArrayList<t_neo4j_node_info_node> nodes = new ArrayList<t_neo4j_node_info_node>();
		String sql = "";
		if ("".equals(node_Id)) {// 如果没有Id那就是全部查询
			sql = "select info.*,node.node_name,node.lable_name from tc_tools.t_neo4j_node_info info left join tc_tools.t_neo4j_node node on info.node_id=node.nid   ";
		} else {
			sql = "select info.*,node.node_name,node.lable_name from tc_tools.t_neo4j_node_info info left join tc_tools.t_neo4j_node node on info.node_id=node.nid where info.id='"
					+ node_Id + "'  ";

		}
		nodes = (ArrayList<t_neo4j_node_info_node>) oracledb.searchnopagesqlclass(sql, new t_neo4j_node_info_node());
		return nodes;
	}
}
