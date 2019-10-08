package com.able.rocketmqstudy.com.able.consumer;

import com.able.rocketmqstudy.com.able.constant.Constants;
import com.able.rocketmqstudy.com.able.order.OrderStep;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * @param
 * @author jipeng
 * @date 2019-09-27 17:57
 */
@Slf4j
public class SeqConsumer {
    public static void main(String[] args) throws Exception {
        /**
         * 1.创建消费者Consumer，指定消费者组名
         * 2.指定Nameserver地址
         * 3.订阅主题Topic和Tag
         * 4.设置回调函数，处理消息
         * 5.启动消费者consumer
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-group");
        consumer.setNamesrvAddr(Constants.NAME_SERVER);
        //订阅主题Topic和Tag
        consumer.subscribe("seq", "*");
        consumer.setMessageModel(MessageModel.BROADCASTING);
        //设置回调函数，处理消息
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                //System.err.println("consumer 接收到的消息为 " + new String(msg.getBody()));
                log.info("接收到的消息队列id:{},订单信息为:{}", msg.getQueueId(), JSONObject.parseObject(new String(msg.getBody()), OrderStep.class));
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        //启动消费者consumer
        consumer.start();


    }
}

