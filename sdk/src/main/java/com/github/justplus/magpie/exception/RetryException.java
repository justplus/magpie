package com.github.justplus.magpie.exception;

/**
 * Created by zhaoliang on 2017/3/28.
 * 如果处理数据过程中出现了异常信息, 需要重试处理的, 需要抛出该异常。框架会捕获该异常信息, 自动放到重试队列进行再处理。
 */
public class RetryException extends Exception {
    public RetryException(String message) {
        super(message);
    }

    public RetryException(Throwable cause) {
        super(cause);
    }

    public RetryException(String message, Throwable cause) {
        super(message, cause);
    }
}
