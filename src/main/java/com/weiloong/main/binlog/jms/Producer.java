package com.weiloong.main.binlog.jms;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RestController;

import javax.jms.Destination;

@Service("producer")
public class Producer {

    @Autowired
    private JmsTemplate jmsTemplate;

    public void sendMessage(Destination destination, final String message) {
        jmsTemplate.convertAndSend(destination, message);
    }
}
