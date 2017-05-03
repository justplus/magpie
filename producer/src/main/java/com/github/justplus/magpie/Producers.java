package com.github.justplus.magpie;

import com.github.justplus.magpie.api.IProducer;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * Created by zhaoliang on 2017/4/12.
 */
public class Producers {
    @Autowired(required = false)
    private List<IProducer> producers;

    public void start() {
        if (producers == null) return;
        for (IProducer produce : producers) {
            produce.produce();
        }
    }
}
