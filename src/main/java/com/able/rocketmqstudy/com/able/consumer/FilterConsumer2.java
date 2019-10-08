package com.able.rocketmqstudy.com.able.consumer;

import com.able.rocketmqstudy.com.able.constant.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @param
 * @author jipeng
 * @date 2019-10-08 19:08
 */
@Slf4j
public class FilterConsumer2 {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("FilterConsumer2-group");
        consumer.setNamesrvAddr(Constants.NAME_SERVER);
        consumer.subscribe("FilterTopic", "B");
        consumer.setMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                log.info("FilterConsumer2 接收到的消息为:{}", new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();

        log.info("消费者启动");
    }
}

