package com.weiloong.main.binlog.entity;

import com.weiloong.main.binlog.mysql.MysqlConnection;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableInfo {
    private static Logger logger = LoggerFactory.getLogger(TableInfo.class);



    /**
     * key:databaseName+""+tableName
     * value:columns name
     */
    private static Map<String, List<String>> columnsMap = new HashMap<String, List<String>>();
    private String host;
    private Integer port;
    private String username;
    private String password;

    public TableInfo(String host,String username,String password,Integer port){
        this.host=host;
        this.username=username;
        this.password=password;
        this.port = port;
        if(columnsMap==null||columnsMap.size()==0){
            //不进行初始化了
            //MysqlConnection mc = new MysqlConnection();
            //mc.setConnection(this.host,this.port,this.username,this.password);
            //columnsMap = mc.getColumns();

        }
    }

    public synchronized  List<String> getMap(String databaseName,String tableName){
        String key = databaseName + "." + tableName;
        Map<String, List<String>>  keyMap =  MysqlConnection.getColumns(databaseName,tableName);
        columnsMap.putAll(keyMap);

        return columnsMap.get(key);

    }

    public List<String> getColumns(String databaseName,String tableName){
        if(StringUtils.isBlank(databaseName)||StringUtils.isBlank(tableName)){
            return null;
        }
        String key = databaseName + "."+tableName;
        List<String> list =null;
        if(!columnsMap.containsKey(key)){
            MysqlConnection.setConnection(this.host,this.port,this.username,this.password);
            list =  getMap(databaseName,tableName);
        }else{
            list=columnsMap.get(key);
            if(list==null||list.size()==0){
                MysqlConnection.setConnection(this.host,this.port,this.username,this.password);
                list =  getMap(databaseName,tableName);
            }

        }
        return list;
    }
}
