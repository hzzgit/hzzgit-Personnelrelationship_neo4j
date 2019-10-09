package been;

/**
 * 关联属性过滤条件表
 * @author Administrator
 *
 */
public class T_neo4j_link_judge {
  private String id;
  private String link_id;
  private String judge_type;
  private String data_type;
  private String left_field;
  private String right_field;
  private String symbol;
  private String add_value;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getLink_id() {
    return link_id;
  }

  public void setLink_id(String link_id) {
    this.link_id = link_id;
  }

  public String getJudge_type() {
    return judge_type;
  }

  public void setJudge_type(String judge_type) {
    this.judge_type = judge_type;
  }

  public String getData_type() {
    return data_type;
  }

  public void setData_type(String data_type) {
    this.data_type = data_type;
  }

  public String getLeft_field() {
    return left_field;
  }

  public void setLeft_field(String left_field) {
    this.left_field = left_field;
  }

  public String getRight_field() {
    return right_field;
  }

  public void setRight_field(String right_field) {
    this.right_field = right_field;
  }

  public String getSymbol() {
    return symbol;
  }

  public void setSymbol(String symbol) {
    this.symbol = symbol;
  }

  public String getAdd_value() {
    return add_value;
  }

  public void setAdd_value(String add_value) {
    this.add_value = add_value;
  }
}
