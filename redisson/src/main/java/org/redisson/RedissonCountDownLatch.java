/**
 * Copyright (c) 2013-2020 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.redisson.api.RCountDownLatch;
import org.redisson.api.RFuture;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;
import org.redisson.pubsub.CountDownLatchPubSub;

/**
 * Distributed alternative to the {@link java.util.concurrent.CountDownLatch}
 *
 * It has a advantage over {@link java.util.concurrent.CountDownLatch} --
 * count can be reset via {@link #trySetCount}.
 *
 * @author Nikita Koksharov
 *
 */
// 分布式执行体的同步等待
// trySetCount()，set anyCountDownLatch 3
//
//awati()，while true死循环，不断的去判断，get anyCountDownLatch，获取到这个值，如果这个值是>0的话，就说明还没有达到指定数量的客户端去执行countDown的操作，就始终陷入一个死循环之中
//
//不是很想给大家去讲那块逻辑，个人认为，redisson源码写的非常好，但是唯一美中不足的一点，就是在这里混入了一些PUBSUB，基于redis去做发布订阅的一些时间，redis怎么能做类似MQ的事情呢？
//
//redis怎么能做发布订阅的事情呢？完全违背了这个技术的初衷
//
//redis作者的喜好，redis应该发展的方向，是可以支持很多的应用场景，锁、队列、发布订阅，但是我觉得这样子搞就是适得其反
//
//await()方法，其实就是陷入一个while true死循环，不断的get anyCountDownLatch的值，如果这个值还是大于0那么就继续死循环，否则的话呢，就退出这个死循环
//
//countDown()，decr anyCountDownLatch，就是每次一个客户端执行countDown操作，其实就是将这个cocuntDownLatch的值递减1就可以了。如果这个值已经小于等于0，del anyCcoutnDownLatch，删除掉他就可以ile；
//
//如果是这个值为0的时候，还会去发布一个消息，publish redisson_countdownlatch__channel__{anyCountDownLatch} 0
//
//可能要花费不少的时间去看里面的细节代码，那块东西我觉得封装的不是特别好，redisson源码别的地方都写的很清晰和漂亮，但是就是PUB/SUB这里，我觉得源码封装的特别不好，易读性不高
//
//看源码的时候，抓大放小，既然已经把这个核心原理都给搞定了，那么其实我们就不用纠结于一些细节了
public class RedissonCountDownLatch extends RedissonObject implements RCountDownLatch {

    private final CountDownLatchPubSub pubSub;

    private final String id;

    protected RedissonCountDownLatch(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.id = commandExecutor.getConnectionManager().getId();
        this.pubSub = commandExecutor.getConnectionManager().getSubscribeService().getCountDownLatchPubSub();
    }

    @Override
    public void await() throws InterruptedException {
        if (getCount() == 0) {
            return;
        }

        RFuture<RedissonCountDownLatchEntry> future = subscribe();
        try {
            commandExecutor.syncSubscriptionInterrupted(future);

            // 如果redis的countdownlatch值还是大于0，就一直等待
            while (getCount() > 0) {
                // waiting for open state
                future.getNow().getLatch().await();
            }
        } finally {
            unsubscribe(future);
        }
    }

    @Override
    public RFuture<Void> awaitAsync() {
        RPromise<Void> result = new RedissonPromise<>();
        RFuture<Long> countFuture = getCountAsync();
        countFuture.onComplete((r, e) -> {
            if (e != null) {
                result.tryFailure(e);
                return;
            }

            RFuture<RedissonCountDownLatchEntry> subscribeFuture = subscribe();
            subscribeFuture.onComplete((res, ex) -> {
                if (ex != null) {
                    result.tryFailure(ex);
                    return;
                }

                await(result, subscribeFuture);
            });
        });
        return result;
    }

    private void await(RPromise<Void> result, RFuture<RedissonCountDownLatchEntry> subscribeFuture) {
        if (result.isDone()) {
            unsubscribe(subscribeFuture);
            return;
        }

        RFuture<Long> countFuture = getCountAsync();
        countFuture.onComplete((r, e) -> {
            if (e != null) {
                unsubscribe(subscribeFuture);
                result.tryFailure(e);
                return;
            }

            if (r == 0) {
                unsubscribe(subscribeFuture);
                result.trySuccess(null);
                return;
            }

            subscribeFuture.getNow().addListener(() -> {
                await(result, subscribeFuture);
            });
        });
    }

    @Override
    public boolean await(long time, TimeUnit unit) throws InterruptedException {
        long remainTime = unit.toMillis(time);
        long current = System.currentTimeMillis();
        if (getCount() == 0) {
            return true;
        }
        RFuture<RedissonCountDownLatchEntry> promise = subscribe();
        if (!promise.await(time, unit)) {
            return false;
        }

        try {
            remainTime -= System.currentTimeMillis() - current;
            if (remainTime <= 0) {
                return false;
            }

            while (getCount() > 0) {
                if (remainTime <= 0) {
                    return false;
                }
                current = System.currentTimeMillis();
                // waiting for open state
                promise.getNow().getLatch().await(remainTime, TimeUnit.MILLISECONDS);

                remainTime -= System.currentTimeMillis() - current;
            }

            return true;
        } finally {
            unsubscribe(promise);
        }
    }

    @Override
    public RFuture<Boolean> awaitAsync(long waitTime, TimeUnit unit) {
        RPromise<Boolean> result = new RedissonPromise<>();

        AtomicLong time = new AtomicLong(unit.toMillis(waitTime));
        long currentTime = System.currentTimeMillis();
        RFuture<Long> countFuture = getCountAsync();
        countFuture.onComplete((r, e) -> {
            if (e != null) {
                result.tryFailure(e);
                return;
            }

            long el = System.currentTimeMillis() - currentTime;
            time.addAndGet(-el);

            if (time.get() <= 0) {
                result.trySuccess(false);
                return;
            }

            long current = System.currentTimeMillis();
            AtomicReference<Timeout> futureRef = new AtomicReference<>();
            RFuture<RedissonCountDownLatchEntry> subscribeFuture = subscribe();
            subscribeFuture.onComplete((res, ex) -> {
                if (ex != null) {
                    result.tryFailure(ex);
                    return;
                }

                if (futureRef.get() != null) {
                    futureRef.get().cancel();
                }

                long elapsed = System.currentTimeMillis() - current;
                time.addAndGet(-elapsed);

                await(time, result, subscribeFuture);
            });
        });
        return result;
    }

    private void await(AtomicLong time, RPromise<Boolean> result, RFuture<RedissonCountDownLatchEntry> subscribeFuture) {
        if (result.isDone()) {
            unsubscribe(subscribeFuture);
            return;
        }

        if (time.get() <= 0) {
            unsubscribe(subscribeFuture);
            result.trySuccess(false);
            return;
        }

        long curr = System.currentTimeMillis();
        RFuture<Long> countFuture = getCountAsync();
        countFuture.onComplete((r, e) -> {
            if (e != null) {
                unsubscribe(subscribeFuture);
                result.tryFailure(e);
                return;
            }

            if (r == 0) {
                unsubscribe(subscribeFuture);
                result.trySuccess(true);
                return;
            }

            long el = System.currentTimeMillis() - curr;
            time.addAndGet(-el);

            if (time.get() <= 0) {
                unsubscribe(subscribeFuture);
                result.trySuccess(false);
                return;
            }

            long current = System.currentTimeMillis();
            AtomicBoolean executed = new AtomicBoolean();
            RedissonCountDownLatchEntry entry = subscribeFuture.getNow();
            AtomicReference<Timeout> futureRef = new AtomicReference<>();
            Runnable listener = () -> {
                executed.set(true);
                if (futureRef.get() != null) {
                    futureRef.get().cancel();
                }

                long elapsed = System.currentTimeMillis() - current;
                time.addAndGet(-elapsed);

                await(time, result, subscribeFuture);
            };
            entry.addListener(listener);

            if (!executed.get()) {
                Timeout timeoutFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) throws Exception {
                        if (entry.removeListener(listener)) {
                            long elapsed = System.currentTimeMillis() - current;
                            time.addAndGet(-elapsed);

                            await(time, result, subscribeFuture);
                        }
                    }
                }, time.get(), TimeUnit.MILLISECONDS);
                futureRef.set(timeoutFuture);
            }
        });
    }

    private RFuture<RedissonCountDownLatchEntry> subscribe() {
        return pubSub.subscribe(getEntryName(), getChannelName());
    }

    private void unsubscribe(RFuture<RedissonCountDownLatchEntry> future) {
        pubSub.unsubscribe(future.getNow(), getEntryName(), getChannelName());
    }

    @Override
    public void countDown() {
        get(countDownAsync());
    }

    @Override
    public RFuture<Void> countDownAsync() {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        // 将锁getName()的值自减1
                        "local v = redis.call('decr', KEYS[1]);" +
                                // 如果state值 小于等于 0 量，就删除getName()
                        "if v <= 0 then redis.call('del', KEYS[1]) end;" +
                                // 如果state值等于0，发布消息到队列
                        "if v == 0 then redis.call('publish', KEYS[2], ARGV[1]) end;",
                    Arrays.<Object>asList(getName(), getChannelName()), CountDownLatchPubSub.ZERO_COUNT_MESSAGE);
    }

    private String getEntryName() {
        return id + getName();
    }

    private String getChannelName() {
        return "redisson_countdownlatch__channel__{" + getName() + "}";
    }

    // 获取state值
    @Override
    public long getCount() {
        return get(getCountAsync());
    }

    @Override
    public RFuture<Long> getCountAsync() {
        // 获取name对应的countdownlatch的值
        return commandExecutor.writeAsync(getName(), LongCodec.INSTANCE, RedisCommands.GET_LONG, getName());
    }

    @Override
    public boolean trySetCount(long count) {
        return get(trySetCountAsync(count));
    }

    // 设置countdownlatch的state值
    @Override
    public RFuture<Boolean> trySetCountAsync(long count) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // 条件成立：说明getName()不存在
                "if redis.call('exists', KEYS[1]) == 0 then "
                        // 设置getName()，指定值为count
                    + "redis.call('set', KEYS[1], ARGV[2]); "
                        // 发布消息
                    + "redis.call('publish', KEYS[2], ARGV[1]); "
                    + "return 1 "
                + "else "
                    + "return 0 "
                + "end",
                Arrays.<Object>asList(getName(), getChannelName()), CountDownLatchPubSub.NEW_COUNT_MESSAGE, count);
    }

    // 删除countdownlatch
    @Override
    public RFuture<Boolean> deleteAsync() {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // 条件成立：说明删除成功
                "if redis.call('del', KEYS[1]) == 1 then "
                        // 发布消息
                    + "redis.call('publish', KEYS[2], ARGV[1]); "
                        // 返回1，删除成功
                    + "return 1 "
                + "else "
                        // 返回0，删除失败
                    + "return 0 "
                + "end",
                Arrays.<Object>asList(getName(), getChannelName()), CountDownLatchPubSub.NEW_COUNT_MESSAGE);
    }

}
