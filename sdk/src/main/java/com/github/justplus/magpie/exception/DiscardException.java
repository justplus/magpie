package com.github.justplus.magpie.exception;

/**
 * Created by zhaoliang on 2017/3/28.
 * 如果处理数据过程时出现了已知的异常信息, 如脏数据等不需要重试处理的, 需要抛出该异常。框架会捕获该异常, 直接放弃继续处理该消息。
 */
public class DiscardException extends Exception {

    public DiscardException(String message) {
        super(message);
    }

    public DiscardException(Throwable cause) {
        super(cause);
    }

    public DiscardException(String message, Throwable cause) {
        super(message, cause);
    }
}
