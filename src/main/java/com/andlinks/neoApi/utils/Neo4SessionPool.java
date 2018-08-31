package com.andlinks.neoApi.utils;

import org.neo4j.driver.v1.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2017/7/22.
 */
@Component
public class Neo4SessionPool {

    @Value("${neo4j.url}")
    private String url;
    @Value("${neo4j.user}")
    private String user;
    @Value("${neo4j.passw}")
    private String passw;

    private volatile Driver driver;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassw() {
        return passw;
    }

    public void setPassw(String passw) {
        this.passw = passw;
    }

    public Driver getDriver() {
        return driver;
    }

    public void setDriver(Driver driver) {
        this.driver = driver;
    }


    @PostConstruct
    private void init() {

        Config.ConfigBuilder builder = Config.build();
        builder.withConnectionTimeout(30, TimeUnit.SECONDS);
        driver = GraphDatabase.driver(url, AuthTokens.basic(user, passw), Config.defaultConfig());
    }

    public Session getConnection() {
        return driver.session();
    }

    @PreDestroy
    public void onDestroy() {
        driver.close();
    }
}
