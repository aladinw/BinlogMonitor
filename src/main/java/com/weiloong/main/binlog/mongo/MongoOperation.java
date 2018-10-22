package com.weiloong.main.binlog.mongo;

import com.mongodb.WriteResult;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.weiloong.main.binlog.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 操作mongo
 */
@RestController
public class MongoOperation {

    private static Logger logger = LoggerFactory.getLogger(MongoOperation.class);
    @Autowired
    private MongoTemplate mongoTemplate;


    public void operationMongo(LogEvent logEvent, Map<String, String> mapType){
        logger.info("---------------------触发同步------------------");
        String tbTableName = logEvent.getTableName();

        Class className = null;

        String mainRegexIntent = "tbBorrowIntent\\d+";
        String mainRegexBill = "tbBorrowerBill\\d+";

        if(tbTableName.matches(mainRegexIntent)){
            className = tbBorrowIntent.class;
        }

        if(tbTableName.matches(mainRegexBill)){
            className = tbBorrowerBill.class;
        }
        if(className==null ){
            logger.info("----tbTableName非同步数据库，不进行同步-----");
            return;
        }

        String strEventType = logEvent.getEventType();

        switch (strEventType){
            case "WRITE_ROWS_EVENT" :
                updateEvent(logEvent,className,mapType);
                break;

            case "DELETE_ROWS_EVENT" :
                delEvent(logEvent,className,mapType);
                break;

            case "UPDATE_ROWS_EVENT" :
                updateEvent(logEvent,className,mapType);
                break;

            default:

        }


    }


    private void updateEvent(LogEvent logEvent,Class className, Map<String, String> mapType){
        long lId = Long.parseLong(logEvent.getBefore().get("lId"));
        logger.info("--数据库:"+className.getName()+",lId:"+lId+"开始触发更新操作;lId:"+lId+"---");
        Query query=new Query(Criteria.where("lId").is(lId));
        Update update = new Update();
        Map<String, String> map =  logEvent.getAfter();

        for (Map.Entry<String, String> entry : map.entrySet()) {
            String key  = entry.getKey();
            Object value  = entry.getValue();


            String type = mapType.get(key);
            if(type.equals("TINYINT")){
                update.set(key,Integer.valueOf(value.toString()));
            }else if(type.equals("SMALLINT")){
                update.set(key,Integer.valueOf(value.toString()));
            }else if(type.equals("MEDIUMINT")){
                update.set(key,Integer.valueOf(value.toString()));
            }else if(type.equals("INT")){
                update.set(key,Integer.valueOf(value.toString()));
            }else if(type.equals("INTEGER")){
                update.set(key,Integer.valueOf(value.toString()));
            }else if(type.equals("BIGINT")){
                update.set(key,Long.parseLong(value.toString()));
            }else {
                update.set(key,value);
            }

        }
        WriteResult ur = mongoTemplate.upsert(query,update,className);
        long nMCount = ur.getN();
        logger.info("--数据库:"+className.getName()+",lId:"+lId+"结束更新操作，影响条数:"+nMCount+" ---");

    }


    private void delEvent(LogEvent logEvent,Class className, Map<String, String> mapType){

        long lId = Long.parseLong(logEvent.getAfter().get("lId"));

        logger.info("--数据库:"+className.getName()+",lId:"+lId+"开始触发删除操作;lId:"+lId+"---");
        Query query=new Query(Criteria.where("lId").is(lId));
        WriteResult dr = mongoTemplate.remove(query,className);
        long nDelCount = dr.getN();
        logger.info("--数据库:"+className.getName()+",lId:"+lId+"结束删除操作，影响条数:"+nDelCount+" ---");
    }
}
