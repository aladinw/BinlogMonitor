package com.weiloong.main.binlog.utils;

import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.weiloong.main.binlog.entity.LogEvent;
import com.weiloong.main.binlog.entity.tbBorrowIntent;
import com.weiloong.main.binlog.entity.tbBorrowerBill;
import com.weiloong.main.binlog.mongo.MongoOperation;
import com.weiloong.main.binlog.entity.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class BinlogUtils {

    @Value("${custom.host}")
    private String host;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;

    @Autowired
    private MongoOperation mongoOperation;


    private static Logger logger = LoggerFactory.getLogger(BinlogUtils.class);

    public  void processBinLog(AbstractRowEvent actualEvent, String eventDatabase, String tableName,List<Pair<Row>>  rows){

        LogEvent logEvent = new LogEvent(actualEvent,eventDatabase,tableName);
        List<Column> cols_after = null;
        List<Column> cols_before = null;
        for(Pair<Row> p : rows){
            Row after = p.getAfter();
            Row before = p.getBefore();
            cols_after = after.getColumns();
            cols_before = before.getColumns();
            break;
        }
        logEvent.setBefore(getMap(cols_before, eventDatabase.trim(), tableName.trim()));
        logEvent.setAfter(getMap(cols_after, eventDatabase.trim(),   tableName.trim()));
        logger.info("update event is:"+logEvent);
        mongoOperation.operationMongo(logEvent,getType(eventDatabase.trim(),   tableName.trim()));
    }

    public  void InsertBinLog(AbstractRowEvent actualEvent, String eventDatabase, String tableName,List<Row>  rows){

        LogEvent logEvent = new LogEvent(actualEvent,eventDatabase,tableName);
        List<Column> cols_after = null;
        List<Column> cols_before = null;
        for(Row p : rows){
            cols_after = p.getColumns();
            cols_before = p.getColumns();
            break;
        }
        logEvent.setBefore(getMap(cols_before, eventDatabase.trim(), tableName.trim()));
        logEvent.setAfter(getMap(cols_after, eventDatabase.trim(),   tableName.trim()));
        mongoOperation.operationMongo(logEvent,getType(eventDatabase.trim(),   tableName.trim()));
    }


    private Map<String, String> getMap(List<Column> cols, String databaseName, String tableName){
        if(cols==null||cols.size()==0){
            return null;
        }
        TableInfo ti = new TableInfo(host,username,password, 3306);
        List<String> columnNames =  ti.getColumns(databaseName, tableName);
        if(columnNames==null){
            return null;
        }
        if(columnNames.size()!=cols.size()){
            logger.error("the size does not match...");
            return null;
        }
        Map<String, String> map = new HashMap<String, String>();
        for(int i=0;i<columnNames.size();i++){
            if(cols.get(i).getValue()==null){
                //拼接  表名##类型
                map.put(columnNames.get(i).toString().split("##")[0],"");
            }else{
                map.put(columnNames.get(i).toString().split("##")[0],cols.get(i).toString());
            }

        }
        return map;
    }


    private Map<String, String> getType(String databaseName, String tableName){

        TableInfo ti = new TableInfo(host,username,password, 3306);
        List<String> columnNames =  ti.getColumns(databaseName, tableName);
        if(columnNames==null){
            return null;
        }

        Map<String, String> map = new HashMap<String, String>();
        for(int i=0;i<columnNames.size();i++){
            map.put(columnNames.get(i).toString().split("##")[0],columnNames.get(i).toString().split("##")[1]);
        }
        return map;
    }

    public String getDatabaseName( String databaseName, String tableName){
        String mainRegexIntent = "tbBorrowIntent\\d+";
        String mainRegexBill = "tbBorrowerBill\\d+";

        if(tableName.matches(mainRegexIntent) ||  tableName.matches(mainRegexBill)){
            int nDB = getTableDB(tableName);
            databaseName = "P2P"+nDB;
        }

        return databaseName;

    }

    private int getTableDB(String input){
        Pattern pattern = Pattern.compile("[^0-9]");
        Matcher matcher = pattern.matcher(input);
        String find_result = matcher.replaceAll("");
        return Integer.parseInt(find_result)/100;
    }
}
