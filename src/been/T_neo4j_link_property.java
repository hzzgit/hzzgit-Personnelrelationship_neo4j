package been;

/**
 * 关联之间属性表
 * @author Administrator
 *
 */
public class T_neo4j_link_property {
  private String nid;
  private String link_id;
  private String oracle_field;
  private String neo4j_name;
  private String show_name;

  public String getNid() {
    return nid;
  }

  public void setNid(String nid) {
    this.nid = nid;
  }

  public String getLink_id() {
    return link_id;
  }

  public void setLink_id(String link_id) {
    this.link_id = link_id;
  }

  public String getOracle_field() {
    return oracle_field;
  }

  public void setOracle_field(String oracle_field) {
    this.oracle_field = oracle_field;
  }

  public String getNeo4j_name() {
    return neo4j_name;
  }

  public void setNeo4j_name(String neo4j_name) {
    this.neo4j_name = neo4j_name;
  }

  public String getShow_name() {
    return show_name;
  }

  public void setShow_name(String show_name) {
    this.show_name = show_name;
  }
}
