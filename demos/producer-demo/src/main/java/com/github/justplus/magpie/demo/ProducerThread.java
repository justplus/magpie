package com.github.justplus.magpie.demo;

import com.github.justplus.magpie.service.producer.AbstractProducer;

import java.util.concurrent.CountDownLatch;

/**
 * Created by zhaoliang on 2017/4/16.
 */
public class ProducerThread extends Thread {
    private CountDownLatch latch;
    private AbstractProducer producer;

    public ProducerThread(CountDownLatch latch, AbstractProducer producer) {
        this.latch = latch;
        this.producer = producer;
    }

    @Override
    public void run() {
        while (true) {
            boolean finished = producer.start();
            if (finished) {
                latch.countDown();
                break;
            }
        }
    }
}