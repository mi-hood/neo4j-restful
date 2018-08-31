package com.andlinks.neoApi.utils;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashSet;
import java.util.Map;

@Component
public class NeoOperations {
    Logger logger = LoggerFactory.getLogger(NeoOperations.class);
    @Autowired
    private Neo4SessionPool neo4SessionPool;

    /**
     * 执行节点操作
     * @param query
     * @return 返回操作的节点
     */
    private HashSet<Node> execute4node(String query) {
        logger.info(query);
        HashSet<Node> result = new HashSet<>();
        Session session = neo4SessionPool.getConnection();
        StatementResult sr = session.run(query);
        while (sr.hasNext()) {
            Record record = sr.next();
            result.add(record.get("node").asNode());
        }
        session.close();
        return result;
    }

    /**
     * 执行删除操作
     *
     * @param query
     * @return 返回是否操作成功
     */
    private boolean delOpeation(String query) {
        logger.info(query);
        Session session = neo4SessionPool.getConnection();
        StatementResult sr = session.run(query);
        if (sr.hasNext()) {
            logger.info(sr.next().asMap().toString());
            return true;
        } else {
            logger.info("deleted over!");
            return true;
        }

    }

    /**
     * 执行关系操作
     * @param query
     * @return 返回操作的关系
     */
    private HashSet<Relationship> execute4relation(String query) {
        logger.info(query);
        HashSet<Relationship> result = new HashSet<>();
        Session session = neo4SessionPool.getConnection();
        StatementResult sr = session.run(query);
        while (sr.hasNext()) {
            Record record = sr.next();
            result.add(record.get("relation").asRelationship());
        }
        session.close();
        return result;
    }

    /**
     * 处理标签参数，防止出现空值
     * @param para
     * @return 返回处理过的参数
     */
    private String paraProcess(String para){
        if(para!=null && !para.equals(""))
            return ":"+para;
        else
            return "";
    }

    /**
     *增加节点
     * @param label 节点类型
     * @param nodePropertis 节点属性键值对
     * @return 返回是否增加成功
     */
    public boolean addNode(String label, Map<String, Object> nodePropertis){
        boolean isOk=false;
        StringBuilder properties=new StringBuilder();
        setPropertiesStr(nodePropertis,properties);
        String cql=String.format("merge (node %s {%s}) return node",paraProcess(label),properties.toString());
        if(execute4node(cql).size()>0)
            isOk=true;
        return isOk;
    }

    /**
     * 删除节点
     *
     * @param label         节点类型
     * @param nodePropertis 节点属性键值对
     * @return 返回是否删除成功
     */
    public boolean delNode(String label, Map<String, Object> nodePropertis) {
        boolean isOk = false;
        StringBuilder properties = new StringBuilder();
        setPropertiesStr(nodePropertis,properties);
        String cql = String.format("match (node %s {%s}) detach  delete node", paraProcess(label), properties.toString());
        return delOpeation(cql);
    }

    /**
     *找到node就新增或修改属性,未找到节点则跳过
     * @param label 节点类型
     * @param nodePropertis  目标节点存在的节点属性
     * @param updateProperties  要增加或修改的节点属性
     * @return 返回是否修改成功
     */
    public boolean updateNode(String label, Map<String, Object> nodePropertis, Map<String, Object> updateProperties){
        boolean isOk=false;
        StringBuilder Props=new StringBuilder();
        StringBuilder updateProps=new StringBuilder();
        setPropertiesStr(nodePropertis,Props);
        updateProperties.forEach((propType, propValue)->{
            if(2>updateProps.toString().length()) {
                updateProps.append("node." + propType + "=" + "\"" + propValue.toString()+"\"");
            } else {
                updateProps.append("," + "node." + propType + "=" + "\"" + propValue.toString()+"\"");
            }
        });
        String cql = String.format("match (node %s {%s}) set %s return node", paraProcess(label), Props.toString(), updateProps.toString());

        if(execute4node(cql).size()>0)
            isOk=true;
        return isOk;
    }

    /**
     * 查询符合给定标签和属性的节点
     * @param label 节点标签
     * @param nodeProp  属性键值对
     * @return  符合条件的节点集合
     */
    public HashSet<Node> queryNode(String label, Map<String, Object> nodeProp){
        StringBuilder Props=new StringBuilder();
        setPropertiesStr(nodeProp,Props);
        String cql = String.format("match (node %s {%s}) return  node", paraProcess(label), Props.toString());
        return execute4node(cql);
    }

    /**
     * 增加关系
     * @param labelOne  节点1类型
     * @param labelTwo  节点2类型
     * @param labelRel  关系类型
     * @param nodeOne   节点1属性
     * @param nodeTwo   节点2属性
     * @param relationProps 关系属性
     * @return  返回是否增加成功
     */
    public boolean addRelation(String labelOne, String labelTwo, String labelRel, Map<String, Object> nodeOne, Map<String, Object> nodeTwo, Map<String, Object> relationProps){
        boolean isOk=false;
        StringBuilder oneProps=new StringBuilder();
        StringBuilder twoProps=new StringBuilder();
        StringBuilder relProps=new StringBuilder();
        setPropertiesStr(nodeOne,oneProps);
        setPropertiesStr(nodeTwo,twoProps);
        setPropertiesStr(relationProps,relProps);
        String cql=String.format("match (nodeOne %s {%s}),(nodeTwo %s {%s}) merge (nodeOne)-[relation %s {%s}]->(nodeTwo) return relation",
                paraProcess(labelOne),oneProps.toString(),paraProcess(labelTwo),twoProps.toString(),paraProcess(labelRel),relProps.toString());
        if(execute4relation(cql).size()>0)
            isOk=true;
        return isOk;
    }

    /**
     * 删除关系
     *
     * @param labelOne      节点1类型
     * @param labelTwo      节点2类型
     * @param labelRel      关系类型
     * @param nodeOne       节点1属性
     * @param nodeTwo       节点2属性
     * @param relationProps 关系属性
     * @return 返回是否增加成功
     */
    public boolean delRelation(String labelOne, String labelTwo, String labelRel, Map<String, Object> nodeOne, Map<String, Object> nodeTwo, Map<String, Object> relationProps) {
        StringBuilder oneProps = new StringBuilder();
        StringBuilder twoProps = new StringBuilder();
        StringBuilder relProps = new StringBuilder();
        setPropertiesStr(nodeOne,oneProps);
        setPropertiesStr(nodeTwo,twoProps);
        setPropertiesStr(relationProps,relProps);
        String cql = String.format("match (nodeOne %s {%s})-[relation %s {%s}]-(nodeTwo %s {%s}) delete relation",
                paraProcess(labelOne), oneProps.toString(), paraProcess(labelRel), relProps.toString(), paraProcess(labelTwo), twoProps.toString());
        return delOpeation(cql);
    }

    /**
     *找到关系并修改或增加属性
     * @param labelOne  节点1类型
     * @param labelTwo  节点2类型
     * @param labelRel  关系类型
     * @param nodeOne   节点1属性
     * @param nodeTwo   节点2属性
     * @param relationProps 关系属性
     * @param updateProps   增加或修改的属性
     * @return  返回是否修改成功
     */
    public boolean updateRelation(String labelOne, String labelTwo, String labelRel, Map<String, Object> nodeOne, Map<String, Object> nodeTwo, Map<String, Object> relationProps, Map<String, Object> updateProps){
        boolean isOk=false;
        StringBuilder oneProps=new StringBuilder();
        StringBuilder twoProps=new StringBuilder();
        StringBuilder relProps=new StringBuilder();
        StringBuilder updateRel=new StringBuilder();
        setPropertiesStr(nodeOne,oneProps);
        setPropertiesStr(nodeTwo,twoProps);
        setPropertiesStr(relationProps,relProps);
        updateProps.forEach((propType, propValue)->{
            if(2>updateRel.toString().length()) {
                updateRel.append("relation." + propType + "=" + "\"" + propValue.toString()+"\"");
            } else {
                updateRel.append("," + "relation." + propType + "=" + "\"" + propValue.toString() + "\"");
            }
        });
        String cql = String.format("match (nodeOne %s {%s})-[relation %s {%s}]-(nodeTwo %s {%s})  set %s return relation",
                paraProcess(labelOne), oneProps.toString(), paraProcess(labelRel), relProps.toString(), paraProcess(labelTwo),twoProps.toString(),updateRel.toString());
        if(execute4relation(cql).size()>0)
            isOk=true;
        return isOk;
    }

    /**
     * 查询关系
     * @param labelOne  节点1类型
     * @param labelTwo  节点2类型
     * @param labelRel  关系类型
     * @param nodeOne   节点1属性
     * @param nodeTwo   节点2属性
     * @param relationProps 关系属性
     * @return  返回是否增加成功
     */
    public HashSet<Relationship> queryRelation(String labelOne, String labelTwo, String labelRel, Map<String, Object> nodeOne, Map<String, Object> nodeTwo, Map<String, Object> relationProps){
        StringBuilder oneProps=new StringBuilder();
        StringBuilder twoProps=new StringBuilder();
        StringBuilder relProps=new StringBuilder();
        setPropertiesStr(nodeOne,oneProps);
        setPropertiesStr(nodeTwo,twoProps);
        setPropertiesStr(relationProps,relProps);
        String cql=String.format("match (nodeOne %s {%s})-[relation %s {%s}]->(nodeTwo %s {%s}) return relation",
                paraProcess(labelOne),oneProps.toString(),paraProcess(labelRel),relProps.toString(),paraProcess(labelTwo),twoProps.toString());
        return execute4relation(cql);
    }

    /**
     * 对给定标签的上的属性列表建立索引。
     * @param label
     * @param props
     */
    public void createIndex(String label, ArrayList<String> props){
        props.forEach(prop->{
            String cql=String.format("create index on :%s(%s)",label,prop);
            execute4node(cql);
        });
    }

    private void setPropertiesStr( Map<String, Object> properties,StringBuilder propertieStr){
        properties.forEach((propType,propValue)->{
            if(2>propertieStr.toString().length()) {
                propertieStr.append(propType + ":" + "\"" + propValue.toString()+"\"");
            } else {
                propertieStr.append("," + propType + ":" + "\"" + propValue.toString()+"\"");
            }
        });
    }
}
