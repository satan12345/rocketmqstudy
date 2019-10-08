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
 * @param
 * @author jipeng
 * @date 2019-10-08 19:03
 */
@Slf4j
public class FilterProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");

        producer.setNamesrvAddr(Constants.NAME_SERVER);

        producer.start();
        for (int i = 0; i < 10; i++) {
            String tag = i % 2 == 0 ? "A" : "B";
            Message message =new Message("FilterTopic",tag,("Tag消息:"+i).getBytes());
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.info("消息发送成:sendResult= {}",sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    log.error("消息发送异常:",e);
                }
            });
        }
        TimeUnit.SECONDS.sleep(2);
        producer.shutdown();

    }
}

