package com.able.rocketmqstudy.com.able.producer;

import com.able.rocketmqstudy.com.able.constant.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * 异步发送消息
 *
 * @param
 * @author jipeng
 * @date 2019-09-27 17:25
 */
@Slf4j
public class AsyncProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");

        producer.setNamesrvAddr(Constants.NAME_SERVER);

        producer.start();

        for (int i = 1; i <=10; i++) {
            String msg = "hello kakaxi " + i;
            Message message = new Message("base", "tag", msg.getBytes());
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.info("消息发送成功:sendResult={}", sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    log.error("消息发送发生异常:e=", e);
                }
            });
        }
        TimeUnit.SECONDS.sleep(2);
        producer.shutdown();

    }
}

