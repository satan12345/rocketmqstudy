package com.able.rocketmqstudy.com.able.consumer;

import com.able.rocketmqstudy.com.able.TimeModel;
import com.able.rocketmqstudy.com.able.constant.Constants;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Date;
import java.util.List;

/**
 * @param
 * @author jipeng
 * @date 2019-10-08 15:46
 */
@Slf4j
public class DelayConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-order-group");
        consumer.setNamesrvAddr(Constants.NAME_SERVER);
        consumer.subscribe("DelayTopic", "*");

        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    log.info("当前时间为：{},接收到的消息为:{}", new Date(), JSONObject.parseObject(msg.getBody(), TimeModel.class));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        log.info("消费者启动");
    }
}

