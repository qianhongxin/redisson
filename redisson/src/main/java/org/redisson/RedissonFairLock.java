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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p>
 * Implements a <b>fair</b> locking so it guarantees an acquire order by threads.
 *
 * @author Nikita Koksharov
 *
 */
// 可重入公平加锁
// 和java的锁一样，redis实现的公平锁也要队列等数据结构支撑，操作数据结构的算法都在lua脚本中，不在redisson客户端代码中
public class RedissonFairLock extends RedissonLock implements RLock {

    private final long threadWaitTime;
    private final CommandAsyncExecutor commandExecutor;
    private final String threadsQueueName;
    private final String timeoutSetName;

    public RedissonFairLock(CommandAsyncExecutor commandExecutor, String name) {
        this(commandExecutor, name, 5000);
    }

    public RedissonFairLock(CommandAsyncExecutor commandExecutor, String name, long threadWaitTime) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.threadWaitTime = threadWaitTime;
        threadsQueueName = prefixName("redisson_lock_queue", name);
        timeoutSetName = prefixName("redisson_lock_timeout", name);
    }

    @Override
    protected RFuture<RedissonLockEntry> subscribe(long threadId) {
        return pubSub.subscribe(getEntryName() + ":" + threadId,
                getChannelName() + ":" + getLockName(threadId));
    }

    @Override
    protected void unsubscribe(RFuture<RedissonLockEntry> future, long threadId) {
        pubSub.unsubscribe(future.getNow(), getEntryName() + ":" + threadId,
                getChannelName() + ":" + getLockName(threadId));
    }

    @Override
    protected RFuture<Void> acquireFailedAsync(long threadId) {
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_VOID,
                // get the existing timeout for the thread to remove
                "local queue = redis.call('lrange', KEYS[1], 0, -1);" +
                // find the location in the queue where the thread is
                "local i = 1;" +
                "while i <= #queue and queue[i] ~= ARGV[1] do " +
                    "i = i + 1;" +
                "end;" +
                // go to the next index which will exist after the current thread is removed
                "i = i + 1;" +
                // decrement the timeout for the rest of the queue after the thread being removed
                "while i <= #queue do " +
                    "redis.call('zincrby', KEYS[2], -tonumber(ARGV[2]), queue[i]);" +
                    "i = i + 1;" +
                "end;" +
                // remove the thread from the queue and timeouts set
                "redis.call('zrem', KEYS[2], ARGV[1]);" +
                "redis.call('lrem', KEYS[1], 0, ARGV[1]);",
                Arrays.<Object>asList(threadsQueueName, timeoutSetName),
                getLockName(threadId), threadWaitTime);
    }

    @Override
    <T> RFuture<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        // 过期时间转换
        internalLockLeaseTime = unit.toMillis(leaseTime);

        long currentTime = System.currentTimeMillis();
        if (command == RedisCommands.EVAL_NULL_BOOLEAN) {
            return evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                    // 使用的参数解释:
                    // KEYS = Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName)
                    // KEYS[1] = getName() = 锁的名字，“name”, 是我们传入的
                    // KEYS[2] = threadsQueueName = redisson_lock_queue:{name}，基于redis的数据结构实现的一个队列
                    // KEYS[3] = timeoutSetName = redisson_lock_timeout:{name}，基于redis的数据结构实现的一个Set数据集合，有序集合，可以自动按照你给每个数据指定的一个分数（score）来进行排序

                    // ARGV[1] = internalLockLeaseTime (30000毫秒)
                    // ARGV[2] = getLockName(threadId) (UUID:threadId)
                    // ARGV[3] = currentTime(当前时间（10:00:00）) + threadWaitTime(5000毫秒) = (10:00:05)
                    // ARGV[4] = currentTime(当前时间（10:00:00）)


                    // remove stale threads
                    // 自旋
                    "while true do " +
                            // 获取队列第一个元素值
                        "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);" +
                            // 如果第一个值是false，表示队列为空，退出循环
                            // 如果为true，跳过该if，执行后面的
                        "if firstThreadId2 == false then " +
                            "break;" +
                        "end;" +
                            // 获取firstThreadId2在zset集合中的得分，即zscore值。是个时间戳
                        "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));" +
                            // 如果timeout小于等于  currentTime(当前时间（10:00:00）) + threadWaitTime(5000毫秒)
                        "if timeout <= tonumber(ARGV[3]) then " +
                            // remove the item from the queue and timeout set
                            // NOTE we do not alter any other timeout
                            // fixme
                            "redis.call('zrem', KEYS[3], firstThreadId2);" +
                            "redis.call('lpop', KEYS[2]);" +
                        "else " +
                            // 退出循环
                            "break;" +
                        "end;" +
                    "end;" + // 这个end是while循环的结束标记

                            // 条件1成立：说明看我们指定的锁的name不存在，即没有客户端加锁
                            // 条件2成立：表示等待队列为空或者第一个元素为当前加锁线程
                                // 条件2.1成立：表示我们等待队列不存在
                                // 条件2.2成立：表示等待队列存在，且队列的第一个元素是当前要加锁的线程
                            // 条件成立，从队列弹出当前线程，开始加锁
                    "if (redis.call('exists', KEYS[1]) == 0) " +
                        "and ((redis.call('exists', KEYS[2]) == 0) " +
                            "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +
                            // 直接将等待队列的左边第一个元素出队
                        "redis.call('lpop', KEYS[2]);" +
                            // 删除zset中对应的加锁线程，因为这个线程已经要去获取锁了
                        "redis.call('zrem', KEYS[3], ARGV[2]);" +

                        // decrease timeouts for all waiting in the queue
                            // 获取redisson_lock_timeout:{name}队列的从第一个到最后一个元素。即获取队列的全部元素
                        "local keys = redis.call('zrange', KEYS[3], 0, -1);" +
                            // 遍历全部元素
                        "for i = 1, #keys, 1 do " +
                            // 将队列元素的得分都减去当前时间戳的值 fixme
                            "redis.call('zincrby', KEYS[3], -tonumber(ARGV[4]), keys[i]);" +
                        "end;" +

                            // 熟悉的加锁逻辑，并设置过期时间
                        "redis.call('hset', KEYS[1], ARGV[2], 1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                            // 加锁成功，返回null
                        "return nil;" +
                    "end;" +

                            // 熟悉的可重入加锁逻辑，并刷新过期时间
                    "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                            // 可重入加锁成功返回null
                        "return nil;" +
                    "end;" +

                            // 加锁失败，返回1
                    "return 1;",
                    Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName),
                    internalLockLeaseTime, getLockName(threadId), currentTime, threadWaitTime);
        }
        if (command == RedisCommands.EVAL_LONG) {
            return evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                    // remove stale threads
                    // 使用的参数解释:
                    // KEYS = Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName)
                    // KEYS[1] = getName() = 锁的名字，“anyLock”, 是我们传入的
                    // KEYS[2] = threadsQueueName = redisson_lock_queue:{name}，基于redis的数据结构实现的一个队列
                    // KEYS[3] = timeoutSetName = redisson_lock_timeout:{name}，基于redis的数据结构实现的一个Set数据集合，有序集合，可以自动按照你给每个数据指定的一个分数（score）来进行排序

                    // ARGV[1] = internalLockLeaseTime (30000毫秒)
                    // ARGV[2] = getLockName(threadId) (UUID:threadId)
                    // ARGV[3] = currentTime(当前时间（10:00:00）) + threadWaitTime(5000毫秒) = (10:00:05)
                    // ARGV[4] = currentTime(当前时间（10:00:00）)

                    // 自旋
                    "while true do " +
                            // 获取等待队列左边第一个元素值即队头，为空返回nil
                        "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);" +
                            // 如果是空则firstThreadId2是nil，nil==false成立，直接退出循环。
                        "if firstThreadId2 == false then " +
                            // 跳出循环
                            "break;" +
                        "end;" +
                            // 走到这，说明队列KEYS[2]不是空，获取到的队头元素firstThreadId2曾今因为加锁失败所以在队列KEYS[2]中等待
                            // redis.call('zscore', KEYS[3], firstThreadId2): 获取redisson_lock_timeout:{name}队列的firstThreadId2的得分，即时间戳值。如果不存在返回nil
                            // 如果返回nil，则tonumber(nil) 为 0，即timeout为0
                        "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));" +
                            // 如果时间 小于等于 当前时间戳，执行出队逻辑，即从KEYS[3]和KEYS[2]中移除掉 fixme
                        "if timeout <= tonumber(ARGV[4]) then " +
                            // remove the item from the queue and timeout set
                            // NOTE we do not alter any other timeout
                            // 从redisson_lock_timeout:{name}删除firstThreadId2
                            "redis.call('zrem', KEYS[3], firstThreadId2);" +
                            // 从redisson_lock_queue:{name}出队左边第一个元素
                            "redis.call('lpop', KEYS[2]);" +
                            // 继续下一次while循环，这个时候KEYS[2]左边第一个元素已经被移除了，下次循环处理下一个元素了
                        "else " +
                            // 跳出循环
                            "break;" +
                        "end;" +
                    "end;" + // while循环结束标示

                    // check if the lock can be acquired now
                            // 条件1成立：说明看我们指定的锁的name不存在，即当前还没人加锁
                            // 条件2成立：表示等待队列为空或者第一个元素为当前加锁线程
                            //      条件2.1成立：表示我们等待队列不存在
                            //      条件2.2成立：表示等待队列存在，且队列的第一个元素即队头是当前要加锁的线程
                    "if (redis.call('exists', KEYS[1]) == 0) " +
                        "and ((redis.call('exists', KEYS[2]) == 0) " +
                            "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +

                        // remove this thread from the queue and timeout set
                            // 从redisson_lock_queue:{name}出队左边第一个元素，即队头元素出队
                        "redis.call('lpop', KEYS[2]);" +
                            // 从redisson_lock_timeout:{name}删除当前线程
                        "redis.call('zrem', KEYS[3], ARGV[2]);" +

                        // decrease timeouts for all waiting in the queue
                            // 获取redisson_lock_timeout:{name}队列的从第一个到最后一个元素。即获取队列的全部元素
                        "local keys = redis.call('zrange', KEYS[3], 0, -1);" +
                            // 遍历全部元素
                        "for i = 1, #keys, 1 do " +
                            // 将队列元素的得分都减去当前时间戳的值 fixme
                            "redis.call('zincrby', KEYS[3], -tonumber(ARGV[3]), keys[i]);" +
                        "end;" +

                        // acquire the lock and set the TTL for the lease
                            // 熟悉的加锁逻辑
                        "redis.call('hset', KEYS[1], ARGV[2], 1);" +
                            // 设置锁的过期时间
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                            // 加锁成功，返回null
                        "return nil;" +
                    "end;" +

                    // check if the lock is already held, and this is a re-entry
                            // 可重入加锁逻辑
                    "if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then " +
                            // 锁自增1
                        "redis.call('hincrby', KEYS[1], ARGV[2],1);" +
                            // 刷新过期时间
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                            // 可重入加锁成功，返回null
                        "return nil;" +
                    "end;" +

                    // the lock cannot be acquired
                    // check if the thread is already in the queue

                    // fixme
                    "local timeout = redis.call('zscore', KEYS[3], ARGV[2]);" +
                    "if timeout ~= false then " +
                        // the real timeout is the timeout of the prior thread
                        // in the queue, but this is approximately correct, and
                        // avoids having to traverse the queue
                        "return timeout - tonumber(ARGV[3]) - tonumber(ARGV[4]);" +
                    "end;" +

                    // add the thread to the queue at the end, and set its timeout in the timeout set to the timeout of
                    // the prior thread in the queue (or the timeout of the lock if the queue is empty) plus the
                    // threadWaitTime
                    // 以下是加锁失败后进入队列排队的逻辑 fixme
                            // 获取KEYS[2]的最后一个元素的值
                    "local lastThreadId = redis.call('lindex', KEYS[2], -1);" +
                            // 定义ttl变量
                    "local ttl;" +
                            // 条件一成立：说明lastThreadId不是空，有值
                            // 条件二成立：说明lastThreadId不等于当前要加锁的客户端的标识
                    "if lastThreadId ~= false and lastThreadId ~= ARGV[2] then " +
                            // 更新score值
                        "ttl = tonumber(redis.call('zscore', KEYS[3], lastThreadId)) - tonumber(ARGV[4]);" +
                    "else " +
                            // 获取锁的剩余过期时间
                        "ttl = redis.call('pttl', KEYS[1]);" +
                    "end;" +
                            // 为当前的客户端计算一个timeout
                    "local timeout = ttl + tonumber(ARGV[3]) + tonumber(ARGV[4]);" +
                            // 条件成立：说明当前客户端标识放入集合KEYS[3]成功
                    "if redis.call('zadd', KEYS[3], timeout, ARGV[2]) == 1 then " +
                            // 将当前客户端从KEYS[2]队列右边入队
                        "redis.call('rpush', KEYS[2], ARGV[2]);" +
                    "end;" +
                            // 返回锁的剩余过期时间，客户端拿到后，如果等待就等待ttl的时间后
                    "return ttl;",
                    Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName),
                    internalLockLeaseTime, getLockName(threadId), threadWaitTime, currentTime);
        }

        throw new IllegalArgumentException();
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // remove stale threads
                "while true do "
                + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                + "if firstThreadId2 == false then "
                    + "break;"
                + "end; "
                + "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));"
                + "if timeout <= tonumber(ARGV[4]) then "
                    + "redis.call('zrem', KEYS[3], firstThreadId2); "
                    + "redis.call('lpop', KEYS[2]); "
                + "else "
                    + "break;"
                + "end; "
              + "end;"
                
              + "if (redis.call('exists', KEYS[1]) == 0) then " + 
                    "local nextThreadId = redis.call('lindex', KEYS[2], 0); " + 
                    "if nextThreadId ~= false then " +
                        "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " +
                    "return 1; " +
                "end;" +
                "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                    "return nil;" +
                "end; " +
                "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                "if (counter > 0) then " +
                    "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                    "return 0; " +
                "end; " +
                    
                "redis.call('del', KEYS[1]); " +
                "local nextThreadId = redis.call('lindex', KEYS[2], 0); " + 
                "if nextThreadId ~= false then " +
                    "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                "end; " +
                "return 1; ",
                Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName, getChannelName()), 
                LockPubSub.UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId), System.currentTimeMillis());
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return deleteAsync(getName(), threadsQueueName, timeoutSetName);
    }

    @Override
    public RFuture<Long> sizeInMemoryAsync() {
        List<Object> keys = Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName);
        return super.sizeInMemoryAsync(keys);
    }

    @Override
    public RFuture<Boolean> expireAsync(long timeToLive, TimeUnit timeUnit) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "redis.call('pexpire', KEYS[2], ARGV[1]); " +
                        "return redis.call('pexpire', KEYS[3], ARGV[1]); ",
                Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName),
                timeUnit.toMillis(timeToLive));
    }

    @Override
    public RFuture<Boolean> expireAtAsync(long timestamp) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        "redis.call('pexpireat', KEYS[1], ARGV[1]); " +
                        "redis.call('pexpireat', KEYS[2], ARGV[1]); " +
                        "return redis.call('pexpireat', KEYS[3], ARGV[1]); ",
                Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName),
                timestamp);
    }

    @Override
    public RFuture<Boolean> clearExpireAsync() {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        "redis.call('persist', KEYS[1]); " +
                        "redis.call('persist', KEYS[2]); " +
                        "return redis.call('persist', KEYS[3]); ",
                Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName));
    }

    
    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // remove stale threads
                "while true do "
                + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                + "if firstThreadId2 == false then "
                    + "break;"
                + "end; "
                + "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));"
                + "if timeout <= tonumber(ARGV[2]) then "
                    + "redis.call('zrem', KEYS[3], firstThreadId2); "
                    + "redis.call('lpop', KEYS[2]); "
                + "else "
                    + "break;"
                + "end; "
              + "end;"
                + 
                
                "if (redis.call('del', KEYS[1]) == 1) then " + 
                    "local nextThreadId = redis.call('lindex', KEYS[2], 0); " + 
                    "if nextThreadId ~= false then " +
                        "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " + 
                    "return 1; " + 
                "end; " + 
                "return 0;",
                Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName, getChannelName()), 
                LockPubSub.UNLOCK_MESSAGE, System.currentTimeMillis());
    }

}