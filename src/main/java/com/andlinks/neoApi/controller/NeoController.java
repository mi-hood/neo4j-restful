package com.andlinks.neoApi.controller;

import com.alibaba.fastjson.JSONObject;
import com.andlinks.neoApi.service.NeoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("neo")
public class NeoController {

    @Autowired
    NeoService neoService;
    @RequestMapping("addNode")
    public String addNode(String nodeJson) {
        if (isValid(nodeJson)) {
            if (!hasLabel(nodeJson))
                return "Please ensure parameter:nodeJson has 'label' property.";
            return neoService.addNode(nodeJson);
        } else {
            return "Please ensure all parameter are valid Json.";
        }
    }

    @RequestMapping("delNode")
    public String delNode(String nodeJson) {
        if (isValid(nodeJson)) {
            if (!hasLabel(nodeJson))
                return "Please ensure parameter:nodeJson has 'label' property.";
            return neoService.delNode(nodeJson);
        } else {
            return "Please ensure all parameter are valid Json.";
        }
    }

    @RequestMapping("queryNode")
    public String queryNode(String nodeJson) {
        if (isValid(nodeJson)) {
            if (!hasLabel(nodeJson))
                return "Please ensure parameter:nodeJson has 'label' property.";
            return neoService.queryNode(nodeJson);
        } else {
            return "Please ensure all parameter are valid Json.";
        }
    }

    @RequestMapping("updateNode")
    public String updateNode(String nodeJson, String updateJson) {
        if (isValid(nodeJson) && isValid(updateJson)) {
            if (!hasLabel(nodeJson))
                return "Please ensure parameter:nodeJson has 'label' property.";
            return neoService.updateNode(nodeJson, updateJson);
        } else {
            return "Please ensure all parameter are valid Json.";
        }
    }

    @RequestMapping("addRelation")
    public String addRelation(String nodeOne, String nodeTwo, String jsonData) {

        if (isValid(nodeOne) && isValid(nodeTwo) && isValid(jsonData)) {
            if (!hasLabel(nodeOne) || !hasLabel(nodeTwo) || !hasLabel(jsonData))
                return "Please ensure parameter：nodeOne,nodeTwo,jsonData has 'label' property.";
            return neoService.addRelation(nodeOne, nodeTwo, jsonData);
        } else {
            return "Please ensure all parameter are valid Json.";
        }
    }

    @RequestMapping("delRelation")
    public String delRelation(String nodeOne, String nodeTwo, String jsonData) {
        if (isValid(nodeOne) && isValid(nodeTwo) && isValid(jsonData)) {
            if (!hasLabel(nodeOne) || !hasLabel(nodeTwo) || !hasLabel(jsonData))
                return "Please ensure parameter：nodeOne,nodeTwo,jsonData has 'label' property.";
            return neoService.delRelation(nodeOne, nodeTwo, jsonData);
        } else {
            return "Please ensure all parameter are valid Json.";
        }
    }

    @RequestMapping("queryRelation")
    public String queryRelation(String nodeOne, String nodeTwo, String jsonData) {
        if (isValid(nodeOne) && isValid(nodeTwo) && isValid(jsonData)) {
            if (!hasLabel(nodeOne) || !hasLabel(nodeTwo) || !hasLabel(jsonData))
                return "Please ensure parameter：nodeOne,nodeTwo,jsonData has 'label' property.";
            return neoService.queryRelation(nodeOne, nodeTwo, jsonData);
        } else {
            return "Please ensure all parameter are valid Json.";
        }
    }

    @RequestMapping("updateRelation")
    public String updateRelation(String nodeOne, String nodeTwo, String jsonData, String updateJson) {
        if (isValid(nodeOne) && isValid(nodeTwo) && isValid(jsonData) && isValid(updateJson)) {
            if (!hasLabel(nodeOne) || !hasLabel(nodeTwo) || !hasLabel(jsonData))
                return "Please ensure all Json：nodeOne,nodeTwo,jsonData has label property.";
            return neoService.updateRelation(nodeOne, nodeTwo, jsonData, updateJson);
        } else {
            return "Please ensure all parameter are valid Json.";
        }
    }

    private boolean isValid(String Json) {
        try {
            JSONObject node = JSONObject.parseObject(Json);

            if (node == null) {
                return false;
            } else {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
    }

    private boolean hasLabel(String Json) {
        JSONObject node = JSONObject.parseObject(Json);
        if (node == null) {
            return false;
        } else if (node.get("label") != null) {
            return true;
        } else {
            return false;
        }
    }
}
