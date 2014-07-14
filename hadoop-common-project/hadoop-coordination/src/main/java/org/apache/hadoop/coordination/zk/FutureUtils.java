package org.apache.hadoop.coordination.zk;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.coordination.CoordinationEngine;

/**
 * A bunch of helper methods that simplify Future interactions.
 */
public class FutureUtils {
  public static <I, R> ListenableFuture<R> onSuccess(ListenableFuture<I> future,
                                                     final Function<I, R> func,
                                                     Executor executor) {
    final SettableFuture<R> result = SettableFuture.create();
    Futures.addCallback(future, new FutureCallback<I>() {
      @Override
      public void onSuccess(@Nullable I input) {
        try {
          result.set(func.apply(input));
        } catch (Throwable t) {
          result.setException(t);
        }
      }

      @Override
      public void onFailure(@Nullable Throwable t) {
        if (t == null)
          result.setException(new IllegalStateException("Null cause"));
        else
          result.setException(t);
      }
    }, executor);
    return result;
  }
}
