package com.able.rocketmqstudy.com.able.producer;

import com.able.rocketmqstudy.com.able.constant.Constants;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * 单向发送消息
 *
 * @param
 * @author jipeng
 * @date 2019-09-27 17:44
 */
public class OneWayProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("group");
        producer.setNamesrvAddr(Constants.NAME_SERVER);
        producer.start();

        for (int i = 0; i < 10; i++) {
            String msg = "你好 ,RocketMQ " + i;
            Message message = new Message("base", "tag3", msg.getBytes());
            producer.sendOneway(message);
        }
        TimeUnit.SECONDS.sleep(1);
        System.err.println("oneway消息发送结束");
        producer.shutdown();

    }
}

