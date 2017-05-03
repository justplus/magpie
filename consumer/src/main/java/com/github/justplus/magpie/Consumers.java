package com.github.justplus.magpie;

import com.github.justplus.magpie.api.IConsumer;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * Created by zhaoliang on 2017/4/13.
 */
public class Consumers {
    @Autowired(required = false)
    private List<IConsumer> consumers;

    public void start() {
        if (consumers == null) return;
        for (IConsumer consumer : consumers) {
            consumer.consume();
        }
    }
}
