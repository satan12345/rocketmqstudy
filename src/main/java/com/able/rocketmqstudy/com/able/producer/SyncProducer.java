package com.able.rocketmqstudy.com.able.producer;

import com.able.rocketmqstudy.com.able.constant.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 发送同步消息
 *
 * @param
 * @author jipeng
 * @date 2019-09-27 16:58
 */
@Slf4j
public class SyncProducer {


    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        /**
         * 1.创建消息生产者producer，并指定生产者组名
         * 2.指定Nameserver地址
         * 3.启动producer
         * 4.创建消息对象，指定主题Topic、Tag和消息体
         * 5.发送消息
         * 6.关闭生产者producer
         */
        DefaultMQProducer producer = new DefaultMQProducer("group1");

        producer.setNamesrvAddr(Constants.NAME_SERVER);

        producer.start();


        for (int i = 0; i < 10; i++) {
            /**
             * topic：消息的主题tpoic
             * tag:消息tag
             * 参数3：消息内容
             */
            String msg = "hello world" + i;
            Message message = new Message("base", "tag1", msg.getBytes());
            SendResult send = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    log.info("arg={}",arg);
                  return mqs.get(1);
                }
            },1);
            TimeUnit.MICROSECONDS.sleep(10);
            log.info("result={}", send);
        }
        TimeUnit.SECONDS.sleep(1);
        producer.shutdown();


    }
}

