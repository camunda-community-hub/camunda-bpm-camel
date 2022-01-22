package org.camunda.bpm.camel.common;

import java.util.concurrent.Callable;

import org.camunda.bpm.engine.OptimisticLockingException;

public class CamundaUtils {

    private static long sleepInMs = 250;
    private static int defaultTimes = 1000;

    public static <V> V retryIfOptimisticLockingException(final Callable<V> action) {
        return retryIfOptimisticLockingException(defaultTimes, action);
    }

    public static <V> V retryIfOptimisticLockingException(int times, final Callable<V> action) {

        OptimisticLockingException lastException = null;
        do {
            try {
                return action.call();
            } catch (OptimisticLockingException e) {
                lastException = e;
                --times;
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            try {
                Thread.sleep(sleepInMs);
            } catch (InterruptedException e) {
                // never minde
            }
        } while (times > 0);

        final StringBuilder message = new StringBuilder();
        message.append("Event after ");
        message.append(times);
        message.append(" attempts (every delayed for ");
        message.append(sleepInMs);
        message.append("ms) an OptimisticLockingException is thrown!");
        if (lastException != null) {
            message.append(" message='");
            message.append(lastException.getMessage());
            message.append('\'');
        }
        throw new OptimisticLockingException(message.toString());

    }

}
