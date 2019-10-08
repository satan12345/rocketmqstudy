package com.able.rocketmqstudy.com.able.producer;

import com.able.rocketmqstudy.com.able.TimeModel;
import com.able.rocketmqstudy.com.able.constant.Constants;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @param
 * @author jipeng
 * @date 2019-10-08 15:38
 */
@Slf4j
public class DelayProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");

        producer.setNamesrvAddr(Constants.NAME_SERVER);

        producer.start();
//   private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
        String string = JSON.toJSONString(new TimeModel(new Date()));
        Message message = new Message("DelayTopic", string.getBytes());
            //消息延迟推送
            message.setDelayTimeLevel(3);
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.info("message={} 发送成功 sendResult={}",message,sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    log.error("message={} 发送消息出现问题,e=",message,e);
                }
            });

        TimeUnit.SECONDS.sleep(3);
        producer.shutdown();
    }
}

