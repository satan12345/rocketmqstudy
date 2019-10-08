package com.able.rocketmqstudy.com.able.consumer;

import com.able.rocketmqstudy.com.able.constant.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @param
 * @author jipeng
 * @date 2019-09-27 17:57
 */
@Slf4j
public class Consumer2 {
    public static void main(String[] args) throws Exception {
        /**
         * 1.创建消费者Consumer，制定消费者组名
         * 2.指定Nameserver地址
         * 3.订阅主题Topic和Tag
         * 4.设置回调函数，处理消息
         * 5.启动消费者consumer
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-group");
        consumer.setNamesrvAddr(Constants.NAME_SERVER);
        //订阅主题Topic和Tag
        consumer.subscribe("base","*");
        //设置回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    int queueId = msg.getQueueId();
                    System.err.println("consumer1 接收到的消息为 "+new String(msg.getBody())+" queueId="+queueId);
                }

                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        //启动消费者consumer
        consumer.start();


    }
}

