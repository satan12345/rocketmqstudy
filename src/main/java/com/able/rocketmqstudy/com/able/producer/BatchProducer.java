package com.able.rocketmqstudy.com.able.producer;

import com.able.rocketmqstudy.com.able.constant.Constants;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 * 批量消息
 *
 * @param
 * @author jipeng
 * @date 2019-10-08 18:08
 */
@Slf4j
public class BatchProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");

        producer.setNamesrvAddr(Constants.NAME_SERVER);

        producer.start();
        List<Message> msgs = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("batchTopic", ("批量消息:" + i).getBytes());
            msgs.add(message);
        }
        SendResult send = producer.send(msgs);
        log.info("批量发送消息结果为:sendResult={}", send);
        producer.shutdown();


    }
}

