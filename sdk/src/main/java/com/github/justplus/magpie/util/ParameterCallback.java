package com.github.justplus.magpie.util;

import java.util.concurrent.Callable;

/**
 * Created by zhaoliang on 2017/4/16.
 */
public class ParameterCallback implements Callable<Integer> {
    private Object parameter;

    public ParameterCallback() {

    }

    public ParameterCallback(Object parameter) {
        this.parameter = parameter;
    }

    @Override
    public Integer call() throws Exception {
        return null;
    }


    public Object getParameter() {
        return parameter;
    }

    public void setParameter(Object parameter) {
        this.parameter = parameter;
    }
}
