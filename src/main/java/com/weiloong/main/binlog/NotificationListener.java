package com.weiloong.main.binlog;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import com.weiloong.main.binlog.entity.tbBorrowIntent;
import com.weiloong.main.binlog.entity.tbBorrowerBill;
import com.weiloong.main.binlog.utils.BinlogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class NotificationListener implements BinlogEventListener {
    
    private static Logger logger = LoggerFactory.getLogger(NotificationListener.class);

    @Value("${custom.host}")
    private String host;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;


       private String eventDatabase;
        private String tableName;

        @Autowired
        private BinlogUtils bu;



       /**
          * 这里只是实现例子，该方法可以自由处理逻辑
          * @param event
          */
               @Override
       public void onEvents(BinlogEventV4 event) {
               Class<?> eventType = event.getClass();
               // 事务开始
               if (eventType == QueryEvent.class) {
                       QueryEvent actualEvent = (QueryEvent) event;
                       this.eventDatabase = actualEvent.getDatabaseName().toString();

                       return;
               }
               if(eventDatabase !=null && (!eventDatabase.contains("P2P") )){
                       return;
               }
               // 只监控指定数据库
               if (eventDatabase != null && !"".equals(eventDatabase.trim())) {
                       if (eventType == TableMapEvent.class) {
                               TableMapEvent actualEvent = (TableMapEvent) event;
                               long tableId = actualEvent.getTableId();
                              this.tableName = actualEvent.getTableName().toString();

                              this.eventDatabase = bu.getDatabaseName(eventDatabase,tableName);

                               logger.info("事件数据表ID：{}， 事件数据库表名称：{}",tableId, tableName);
                       } else if (eventType == WriteRowsEvent.class) { // 插入事件
                               WriteRowsEvent actualEvent = (WriteRowsEvent) event;
                               long tableId = actualEvent.getTableId();
                               logger.info("写行事件ID：{}",tableId);
                           bu.InsertBinLog(actualEvent,eventDatabase,tableName,actualEvent.getRows());
                       } else if (eventType == UpdateRowsEvent.class) { // 更新事件
                               UpdateRowsEvent actualEvent = (UpdateRowsEvent) event;
                               long tableId = actualEvent.getTableId();
                               logger.info("更新事件ID：{}",tableId);

                               bu.processBinLog(actualEvent,eventDatabase,tableName,actualEvent.getRows());

                       } else if (eventType == DeleteRowsEvent.class) {// 删除事件
                               DeleteRowsEvent actualEvent = (DeleteRowsEvent) event;
                               long tableId = actualEvent.getTableId();
                               logger.info("删除事件ID：{}",tableId);
                           bu.InsertBinLog(actualEvent,eventDatabase,tableName,actualEvent.getRows());
                       } else if (eventType == XidEvent.class) {// 结束事务
                               XidEvent actualEvent = (XidEvent) event;
                               long xId = actualEvent.getXid();
                               logger.info("结束事件ID：{}",xId);
                       }
               }
       }



}
