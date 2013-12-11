package com.xingcloud.dumpredis;

import com.xingcloud.delayserver.util.Constants;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * User: IvyTang
 * Date: 13-3-4
 * Time: 下午5:21
 */
public class ScpParseExecutor extends ThreadPoolExecutor {

    public ScpParseExecutor() {
        super(Constants.SCPPARSER_EXECUTOR_WORKTHREADS, Constants.SCPPARSER_EXECUTOR_WORKTHREADS, 2, TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());
    }
}
