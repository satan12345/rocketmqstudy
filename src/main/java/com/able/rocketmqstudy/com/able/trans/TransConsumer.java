package com.able.rocketmqstudy.com.able.trans;

import com.able.rocketmqstudy.com.able.constant.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @param
 * @author jipeng
 * @date 2019-10-09 13:16
 */
@Slf4j
public class TransConsumer {
    public static void main(String[] args) throws MQClientException {
        //1.创建消费者Consumer，制定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        //2.指定Nameserver地址
        consumer.setNamesrvAddr(Constants.NAME_SERVER);
        //3.订阅主题Topic和Tag
        consumer.subscribe("transTopic", MessageSelector.byTag(null));

        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    String string=new String(msg.getBody());
                    log.info("消费者接收到的消息为:{}",string);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        //5.启动消费者consumer
        consumer.start();
        System.out.println("消费者启动");
    }
}

