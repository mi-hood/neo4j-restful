package com.andlinks.neoApi.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.andlinks.neoApi.utils.NeoOperations;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@Service
public class NeoService {
    @Autowired
    NeoOperations neoOperations;

    public String addNode(String nodeJson) {
        JSONObject node = JSON.parseObject(nodeJson);
        String label = node.remove("label").toString();
        return neoOperations.addNode(label, node.getInnerMap()) + "";
    }

    public String delNode(String nodeJson) {
        JSONObject node = JSON.parseObject(nodeJson);
        String label = node.remove("label").toString();
        return neoOperations.delNode(label, node.getInnerMap()) + "";
    }

    public String queryNode(String nodeJson) {
        JSONObject node = JSON.parseObject(nodeJson);
        String label = node.remove("label").toString();
        HashSet<Node> r = neoOperations.queryNode(label, node.getInnerMap());
        ArrayList<Map<String, Object>> res = new ArrayList<>();
        r.forEach(n -> {
            res.add(n.asMap());
        });
        return JSON.toJSONString(res);
    }

    public String updateNode(String nodeJson, String updateJson) {
        JSONObject node = JSON.parseObject(nodeJson);
        JSONObject update = JSON.parseObject(updateJson);
        String label = node.remove("label").toString();
        return neoOperations.updateNode(label, node.getInnerMap(), update.getInnerMap()) + "";
    }

    public String addRelation(String nodeOne, String nodeTwo, String jsonData) {
        JSONObject one = JSON.parseObject(nodeOne);
        JSONObject two = JSON.parseObject(nodeTwo);
        JSONObject rel = JSON.parseObject(jsonData);
        String labelOne = one.remove("label").toString();
        String labelTwo = two.remove("label").toString();
        String labelRel = rel.remove("label").toString();
        return neoOperations.addRelation(labelOne, labelTwo, labelRel, one.getInnerMap(), two.getInnerMap(), rel.getInnerMap()) + "";
    }

    public String delRelation(String nodeOne, String nodeTwo, String jsonData) {
        JSONObject one = JSON.parseObject(nodeOne);
        JSONObject two = JSON.parseObject(nodeTwo);
        JSONObject rel = JSON.parseObject(jsonData);
        String labelOne = one.remove("label").toString();
        String labelTwo = two.remove("label").toString();
        String labelRel = rel.remove("label").toString();
        return neoOperations.delRelation(labelOne, labelTwo, labelRel, one.getInnerMap(), two.getInnerMap(), rel.getInnerMap()) + "";
    }

    public String queryRelation(String nodeOne, String nodeTwo, String jsonData) {
        JSONObject one = JSON.parseObject(nodeOne);
        JSONObject two = JSON.parseObject(nodeTwo);
        JSONObject rel = JSON.parseObject(jsonData);
        String labelOne = one.remove("label").toString();
        String labelTwo = two.remove("label").toString();
        String labelRel = rel.remove("label").toString();
        HashSet<Relationship> relationships = neoOperations.queryRelation(labelOne, labelTwo, labelRel, one.getInnerMap(), two.getInnerMap(), rel.getInnerMap());
        ArrayList<Map<String, Object>> res = new ArrayList<>();
        relationships.forEach(relationship -> {
            res.add(relationship.asMap());
        });
        return JSON.toJSONString(res);
    }

    public String updateRelation(String nodeOne, String nodeTwo, String jsonData, String updateJson) {
        JSONObject one = JSON.parseObject(nodeOne);
        JSONObject two = JSON.parseObject(nodeTwo);
        JSONObject rel = JSON.parseObject(jsonData);
        JSONObject update = JSON.parseObject(updateJson);
        String labelOne = one.remove("label").toString();
        String labelTwo = two.remove("label").toString();
        String labelRel = rel.remove("label").toString();
        return neoOperations.updateRelation(labelOne, labelTwo, labelRel, one.getInnerMap(), two.getInnerMap(), rel.getInnerMap(), update.getInnerMap()) + "";
    }
}
