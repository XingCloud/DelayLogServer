package com.xingcloud.delayserver.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * User: IvyTang
 * Date: 13-1-8
 * Time: 下午6:12
 */
public class Helper {

    private static final Log LOG = LogFactory.getLog(Helper.class);

    public static String getDate(long timestamp) {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        df.setTimeZone(TimeZone.getTimeZone(Constants.TIMEZONE));
        Date date = new Date(timestamp);
        return df.format(date);
    }

    public static long getTimestamp(String dateStr) throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        df.setTimeZone(TimeZone.getTimeZone(Constants.TIMEZONE));
        Date date = df.parse(dateStr);
        return date.getTime();
    }

    public static void execShell(String cmd) {
        try {
            Runtime runtime = Runtime.getRuntime();
            String[] cmds = new String[]{"/bin/sh", "-c", cmd};
            Process process = runtime.exec(cmds);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String cmdOutput = null;
            while ((cmdOutput = bufferedReader.readLine()) != null)
                LOG.info(cmdOutput);
            int result = process.waitFor();
            if (result != 0)
                LOG.error("error:" + cmd);
            else
                LOG.info("succeeds:" + cmd);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
}
