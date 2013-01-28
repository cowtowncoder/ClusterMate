package com.fasterxml.clustermate.client.util;

public class ExceptionUtil
{
    public static Throwable peel(Throwable t)
    {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    public static String getStackTraceDesc(Throwable t, int maxTraces)
    {
        StringBuilder sb = new StringBuilder();
        StackTraceElement[] elems = t.getStackTrace();
        for (int i = 0, end = Math.min(elems.length, maxTraces); i < end; ++i) {
            if (i > 0) {
                sb.append("\n");
            }
            StackTraceElement elem = elems[i];
            sb.append(elem.getClassName())
                .append('.').append(elem.getMethodName())
                .append('(').append(elem.getLineNumber()).append(')');
        }
        int left = elems.length - maxTraces;
        if (left > 0) {
            sb.append("[").append(left).append(" traces omitted]");
        }
        return sb.toString();
    }
}
