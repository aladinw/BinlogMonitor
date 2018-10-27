package com.weiloong.main.binlog.mongo;

import com.mongodb.WriteResult;
import com.weiloong.main.binlog.entity.*;
import com.weiloong.main.binlog.jms.Producer;
import com.weiloong.main.binlog.utils.DateUtils;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.web.bind.annotation.RestController;

import javax.jms.Destination;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * 操作mongo
 */
@RestController
public class MongoOperation {

    private static Logger logger = LoggerFactory.getLogger(MongoOperation.class);
    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private Producer producer;


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

        Destination destination = new ActiveMQQueue("mytest.queue");
        producer.sendMessage(destination, "Binlog监听，触发消息");

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

        try {
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
                }else if(type.equals("BIGINT") && !"".equals(value)){
                    update.set(key,Long.parseLong(value.toString()));
                }else if (type.equals("DATETIME") && !"".equals(value)){
                    SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", java.util.Locale.US);
                    Date date = sdf.parse(String.valueOf(value));

                    value = DateUtils.parseDate(date,"yyyy-MM-dd HH:mm:ss");
                    update.set(key,String.valueOf(value));
                }
                else {
                    update.set(key,value);
                }

            }
            WriteResult ur = mongoTemplate.upsert(query,update,className);
            long nMCount = ur.getN();
            logger.info("--数据库:"+className.getName()+",lId:"+lId+"结束更新操作，影响条数:"+nMCount+" ---");

        }catch(Exception e){
            e.printStackTrace();
        }


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
