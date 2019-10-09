package been;

/**
 * 节点信息表
 * @author Administrator
 *
 */
public class T_neo4j_node_info {
  private String id;
  private String show_name;
  private String node_id;
  private String user_name;
  private String table_name;
  private String unique_field;
  private String bulking_field;
  private String bulking_type;
  private String bulking_val;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getShow_name() {
    return show_name;
  }

  public void setShow_name(String show_name) {
    this.show_name = show_name;
  }

  public String getNode_id() {
    return node_id;
  }

  public void setNode_id(String node_id) {
    this.node_id = node_id;
  }

  public String getUser_name() {
    return user_name;
  }

  public void setUser_name(String user_name) {
    this.user_name = user_name;
  }

  public String getTable_name() {
    return table_name;
  }

  public void setTable_name(String table_name) {
    this.table_name = table_name;
  }

  public String getUnique_field() {
    return unique_field;
  }

  public void setUnique_field(String unique_field) {
    this.unique_field = unique_field;
  }

  public String getBulking_field() {
    return bulking_field;
  }

  public void setBulking_field(String bulking_field) {
    this.bulking_field = bulking_field;
  }

  public String getBulking_type() {
    return bulking_type;
  }

  public void setBulking_type(String bulking_type) {
    this.bulking_type = bulking_type;
  }

  public String getBulking_val() {
    return bulking_val;
  }

  public void setBulking_val(String bulking_val) {
    this.bulking_val = bulking_val;
  }
}
