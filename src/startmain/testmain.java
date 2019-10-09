// package startmain;
//
// import java.util.HashSet;
// import java.util.Map;
// import Neo4jdao.Neo4jDrivertest;
//
//
// public class testmain {
//
// public static void main(String[] args) {
//
// String cql =
// "match (p1:Persons) where p1.sfzh='350000000011742266'  match (p2:Persons)  match p= (p1)-[r:qsgx]-(p2)  return p";
//
//
// Neo4jDrivertest neo4jDrivertest =
// new Neo4jDrivertest("bolt://192.168.16.241:7687", "neo4j", "tch123456");
//
// Map<String, HashSet<Map<String, Object>>> test = neo4jDrivertest.printJSON(cql);
// HashSet<Map<String, Object>> nodes = test.get("nodes");
// System.out.println(nodes);
// }
// }
