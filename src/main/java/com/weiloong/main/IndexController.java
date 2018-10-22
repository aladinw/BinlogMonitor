package com.weiloong.main;

import com.weiloong.main.binlog.AutoOpenReplicator;
import com.weiloong.main.binlog.NotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IndexController  {

    private static Logger logger = LoggerFactory.getLogger(IndexController.class);
    @Value("${custom.host}")
    private String host;

    @Value("${spring.datasource.username}")
    private String User;

    @Value("${spring.datasource.password}")
    private String Password;

    private boolean isStartBinlig = false;


    @RequestMapping("/index")
    public String index(){
        return "index";
    }

    @Autowired
    private  AutoOpenReplicator aor;

    @Autowired
    private  NotificationListener nl;

    @RequestMapping("/binlog")
    public String binlog()
    {
        try {
            if(!isStartBinlig){
                logger.info("-------------------开始启动-------------------------");
                // 配置从MySQL Master进行复制
                aor.setServerId(1000701);
                aor.setHost(host);
                aor.setUser(User);
                aor.setPassword(Password);
                aor.setAutoReconnect(true);
                aor.setDelayReconnect(5);
                aor.setBinlogEventListener(nl);
                aor.start();
                logger.info("-------------------启动成功.-------------------------");
                isStartBinlig = true;
            }

        }catch(Exception e){
            throw  e;
        }
        return "index";
    }
}
