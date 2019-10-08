package com.able.rocketmqstudy.com.able;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @param
 * @author jipeng
 * @date 2019-10-08 17:40
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TimeModel {

    private Date currentTime;

    @Override
    public String toString() {
        return com.google.common.base.MoreObjects.toStringHelper(this)
                .add("currentTime", currentTime)
                .toString();
    }
}

