package com.wangbin.collector.core.connection.dispatch;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 通用批量消息调度器，提供批量分派与背压控制。
 */
@Slf4j
public class MessageBatchDispatcher<T> implements AutoCloseable {

    private final BlockingQueue<T> queue;
    private final int batchSize;
    private final long flushIntervalMillis;
    private final OverflowStrategy overflowStrategy;
    private final CopyOnWriteArrayList<Consumer<List<T>>> listeners = new CopyOnWriteArrayList<>();
    private final Thread workerThread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public MessageBatchDispatcher(int capacity,
                                  int batchSize,
                                  long flushIntervalMillis,
                                  OverflowStrategy overflowStrategy) {
        this.queue = new LinkedBlockingQueue<>(Math.max(1, capacity));
        this.batchSize = Math.max(1, batchSize);
        this.flushIntervalMillis = Math.max(0L, flushIntervalMillis);
        this.overflowStrategy = overflowStrategy != null ? overflowStrategy : OverflowStrategy.BLOCK;
        this.workerThread = new Thread(this::processLoop, "dispatcher-" + System.identityHashCode(this));
        this.workerThread.setDaemon(true);
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            workerThread.start();
        }
    }

    public void stop() {
        running.set(false);
        workerThread.interrupt();
        queue.clear();
    }

    public void addListener(Consumer<List<T>> listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    public void removeListener(Consumer<List<T>> listener) {
        if (listener != null) {
            listeners.remove(listener);
        }
    }

    public void enqueue(T item) throws InterruptedException {
        if (item == null) {
            return;
        }
        switch (overflowStrategy) {
            case DROP_LATEST:
                queue.offer(item);
                break;
            case DROP_OLDEST:
                while (!queue.offer(item)) {
                    queue.poll();
                }
                break;
            case BLOCK:
            default:
                queue.put(item);
                break;
        }
    }

    public void clear() {
        queue.clear();
    }

    private void processLoop() {
        List<T> batch = new ArrayList<>(batchSize);
        while (running.get()) {
            try {
                T first = fetchNext();
                if (first == null) {
                    continue;
                }
                batch.add(first);
                queue.drainTo(batch, batchSize - batch.size());

                if (batchSize > 1 && flushIntervalMillis > 0 && batch.size() < batchSize) {
                    long deadline = System.currentTimeMillis() + flushIntervalMillis;
                    while (batch.size() < batchSize && running.get()) {
                        long waitTime = deadline - System.currentTimeMillis();
                        if (waitTime <= 0) {
                            break;
                        }
                        T next = queue.poll(waitTime, TimeUnit.MILLISECONDS);
                        if (next == null) {
                            break;
                        }
                        batch.add(next);
                    }
                }

                dispatchBatch(batch);
                batch.clear();
            } catch (InterruptedException e) {
                if (!running.get()) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (Exception ex) {
                log.warn("消息批量调度异常", ex);
            }
        }
    }

    private T fetchNext() throws InterruptedException {
        if (!running.get()) {
            return null;
        }
        if (flushIntervalMillis > 0) {
            return queue.poll(flushIntervalMillis, TimeUnit.MILLISECONDS);
        }
        return queue.take();
    }

    private void dispatchBatch(List<T> batch) {
        if (batch.isEmpty() || listeners.isEmpty()) {
            return;
        }
        List<T> snapshot = Collections.unmodifiableList(new ArrayList<>(batch));
        for (Consumer<List<T>> listener : listeners) {
            try {
                listener.accept(snapshot);
            } catch (Exception ex) {
                log.warn("消息监听处理失败", ex);
            }
        }
    }

    @Override
    public void close() {
        stop();
    }
}
