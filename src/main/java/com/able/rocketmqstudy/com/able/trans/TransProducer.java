package com.able.rocketmqstudy.com.able.trans;

import com.able.rocketmqstudy.com.able.constant.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.TimeUnit;

/**
 * @param
 * @author jipeng
 * @date 2019-10-09 11:28
 */
@Slf4j
public class TransProducer {


    public static void main(String[] args) throws MQClientException, InterruptedException {

        String[] tags = {"TAGA", "TAGB", "TAGC"};
        //1.创建消息生产者producer，并制定生产者组名
        TransactionMQProducer producer = new TransactionMQProducer("transGroup");
        //2.指定Nameserver地址
        producer.setNamesrvAddr(Constants.NAME_SERVER);
        //设置事务监听
        producer.setTransactionListener(new TransactionListener() {
            /**
             * 执行本地事务
             * @param msg
             * @param arg
             * @return
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                String msgTag = msg.getTags();
                log.info("arg={},msgTag={}",arg,msgTag);
                if (StringUtils.equalsIgnoreCase(msgTag, tags[0])) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                }
                if (StringUtils.equalsIgnoreCase(msgTag, tags[1])) {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
                if (StringUtils.equalsIgnoreCase(msgTag, tags[2])) {
                    return LocalTransactionState.UNKNOW;
                }
                return null;
            }

            /**
             * 本地事务状态的
             * @param msg
             * @return
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                String msgTags = msg.getTags();
                log.info("checkLocalTransaction查询到的Tag={}", msgTags);
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        //3.启动producer
        producer.start();
        for (int i = 0; i < 3; i++) {

            Message message = new Message("transTopic", tags[i], ("hello world " + tags[i]).getBytes());
            TransactionSendResult transactionSendResult = producer.sendMessageInTransaction(message, "ABC"+i);
            log.info("sendStatus={}", transactionSendResult.getSendStatus());
            TimeUnit.SECONDS.sleep(1);
        }

//        TimeUnit.SECONDS.sleep(5);
//        producer.shutdown();


    }
}

