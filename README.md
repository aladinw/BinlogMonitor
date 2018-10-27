# Mysql Binglog Monitor Synchronization Mongo

### 使用开源OpenReplicator，对Mysql binglig进行监听，然后同步至mongo


* 启动后,浏览器输入开启监听:   
    > http://127.0.0.1:7777/binlog

* mongo相关注意
    > 目前程序是同步tbBorrowIntent与tbBorrowerBill两张集合
       程序中需创建此两个集合的实体,只需给一个主键iD即可
       如果需要同步其他几个,需对程序中进行修改