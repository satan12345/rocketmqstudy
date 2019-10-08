package com.able.rocketmqstudy.com.able.producer;

import com.able.rocketmqstudy.com.able.constant.Constants;
import com.able.rocketmqstudy.com.able.order.OrderStep;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 顺序消费
 *
 * @param
 * @author jipeng
 * @date 2019-10-08 12:35
 */
@Slf4j
public class SeqProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");

        producer.setNamesrvAddr(Constants.NAME_SERVER);

        producer.start();

        List<OrderStep> orderSteps = OrderStep.buildOrders();
        for (OrderStep orderStep : orderSteps) {
            Message message = new Message("seq", "", JSON.toJSONString(orderStep).getBytes());
            producer.send(message, (mqs, msg, arg) -> {
                Long orderId = (Long) arg;
                Long l = orderId % mqs.size();
                log.info("订单{}选择的队列序号为:{}", orderStep, l);
                return mqs.get(l.intValue());
            }, orderStep.getOrderId(), new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.info("顺序性消息发成功,sendResult={}", sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    log.error("顺序性消息发送失败:", e);
                }
            });
            TimeUnit.MICROSECONDS.sleep(20);
        }
        TimeUnit.SECONDS.sleep(20);
        producer.shutdown();

    }
}

