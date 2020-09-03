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
// 可重入公平加锁。属于独占锁
// 和java的锁一样，redis实现的公平锁也要队列等数据结构支撑，操作数据结构的算法都在lua脚本中，不在redisson客户端代码中
// 注意：java的公平锁是这样的，当线程加锁时，队列有元素，直接入队。当持锁线程释放锁时，会自己唤醒队头元素抢锁。但是redisson是客户端和服务器redis交互
    //   当客户端请求锁时，如果抢失败，或队列已经有等待客户端，直接入队等待。但是当锁被释放时，队列中等待的客户端需要自己客户端自己的进程向redis请求加锁才会执行公平锁逻辑，所以会有个请求的
    //    的过程，不向单机的java，直接唤醒对应线程即可。redis的客户端散落在各地，他没有通知，而是让客户端请求的方式实现
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
                    // ARGV[3] = currentTime(比如当前时间（10:00:00）) + threadWaitTime(5000毫秒) = (10:00:05)
                    // ARGV[4] = currentTime(比如当前时间（10:00:00）)


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
                    // KEYS[1] = getName() 比如 锁的名字，“anyLock”, 是我们传入的
                    // KEYS[2] = threadsQueueName 比如 redisson_lock_queue:{anyLock}，基于redis的数据结构实现的一个队列
                    // KEYS[3] = timeoutSetName 比如 redisson_lock_timeout:{anyLock}，基于redis的数据结构实现的一个Set数据集合，有序集合，可以自动按照你给每个数据指定的一个分数（score）来进行排序

                    // ARGV[1] = internalLockLeaseTime (比如 30000毫秒)
                    // ARGV[2] = getLockName(threadId) (比如 UUID1:threadId1)
                    // ARGV[3] = currentTime(比如当前时间（10:00:00）) + threadWaitTime(5000毫秒) = (10:00:05)
                    // ARGV[4] = currentTime(比如当前时间（10:00:00）)

                    // 自旋
                    "while true do " +
                            // 获取等待队列左边第一个元素值即队头，为空返回nil。
                            // 本质firstThreadId2是ARGV[2]，即UUID1:threadId1这种
                        "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);" +
                            // 如果是空则firstThreadId2是nil，nil==false成立，直接退出循环。
                        "if firstThreadId2 == false then " +
                            // 跳出循环
                            "break;" +
                        "end;" +
                            // 走到这，说明队列KEYS[2]不是空，获取到的队头元素firstThreadId2曾今因为加锁失败所以在队列KEYS[2]中等待
                            // redis.call('zscore', KEYS[3], firstThreadId2): 获取redisson_lock_timeout:{anyLock}队列的firstThreadId2的得分，即时间戳值。如果不存在返回nil
                            // 如果返回nil，则tonumber(nil) 为 0，即timeout为0
                        "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));" +
                            // 协助 timeout 小于等于 当前时间戳的firstThreadId2做出队逻辑，即从KEYS[3]和KEYS[2]中移除掉 firstThreadId2
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
                            // 条件1成立：说明看我们指定的锁的 anyLock 不存在，即当前还没人加锁
                            // 条件2成立：表示等待队列为空或者第一个元素为当前加锁线程
                            //      条件2.1成立：表示我们等待队列redisson_lock_queue:{anyLock}不存在
                            //      条件2.2成立：表示等待队列redisson_lock_queue:{anyLock}存在，且队列的第一个元素即队头是当前要加锁的线程 即UUID1:threadId1这种
                    "if (redis.call('exists', KEYS[1]) == 0) " +
                        "and ((redis.call('exists', KEYS[2]) == 0) " +
                            "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +
                            // 走到这，说明当前anyLock中还没人加锁,而且对头元素就是当前要加锁的客户端
                            // remove this thread from the queue and timeout set
                            // 从redisson_lock_queue:{anyLock}出队左边第一个元素，即队头元素出队。这个元素就是当前请求的客户端即UUID1:threadId1
                        "redis.call('lpop', KEYS[2]);" +
                            // 从redisson_lock_timeout:{anyLock}删除当前客户端 UUID1:threadId1
                        "redis.call('zrem', KEYS[3], ARGV[2]);" +

                        // decrease timeouts for all waiting in the queue
                            // 获取redisson_lock_timeout:{anyLock}队列的从第一个到最后一个元素。即获取队列的全部元素
                        "local keys = redis.call('zrange', KEYS[3], 0, -1);" +
                            // 遍历全部元素
                        "for i = 1, #keys, 1 do " +
                            // 将元素的得分都减去当前时间戳的值 fixme
                            "redis.call('zincrby', KEYS[3], -tonumber(ARGV[3]), keys[i]);" +
                        "end;" +

                        // acquire the lock and set the TTL for the lease
                            // 熟悉的加锁逻辑，给当前客户端加锁，也就是对头的出来的客户端，上面说了。（这就是公平锁）
                        "redis.call('hset', KEYS[1], ARGV[2], 1);" +
                            // 设置锁的过期时间
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                            // 加锁成功，返回null
                        "return nil;" +
                    "end;" +

                    // check if the lock is already held, and this is a re-entry
                            // 走到这，说明236行if判断失败，即anyLock已经有人加锁了；或者anyLock没人加锁，但是等待队列的对头元素不是当前要加锁的客户端

                            // 条件成立：说明anyLock的已经加锁的客户端就是当前请求的客户端，直接进入可重入加锁逻辑
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

                            // 走到这，说明236行if判断失败，即anyLock已经有人加锁了；或者anyLock没人加锁，但是等待队列的对头元素不是当前要加锁的客户端
                            // 并且268行if判断失败，即当前持有锁的客户端不是当前请求的客户端
                            // 从redisson_lock_timeout:{anyLock}中获取当前请求客户端的得分，即等待时间
                    "local timeout = redis.call('zscore', KEYS[3], ARGV[2]);" +
                            // 条件成立，表示当前请求客户端已经在队列中排队了
                    "if timeout ~= false then " +
                        // the real timeout is the timeout of the prior thread
                        // in the queue, but this is approximately correct, and
                        // avoids having to traverse the queue
                            // 返回当前客户端剩余过期时间 （因为redisson_lock_timeout:{anyLock}中的score是ARGV[3], 为啥这么算看下面入队的算法）
                        "return timeout - tonumber(ARGV[3]) - tonumber(ARGV[4]);" +
                    "end;" +

                    // add the thread to the queue at the end, and set its timeout in the timeout set to the timeout of
                    // the prior thread in the queue (or the timeout of the lock if the queue is empty) plus the
                    // threadWaitTime
                            // 走到这，说明236行if判断失败，即anyLock已经有人加锁了；或者anyLock没人加锁，但是等待队列的对头元素不是当前要加锁的客户端
                            // 并且285行if判断失败，即当前请求客户端不在队列中排队，这里需要执行入队逻辑

                            // 以下是加锁失败后进入队列排队的逻辑
                            // 获取KEYS[2]的最后一个等待客户端
                    "local lastThreadId = redis.call('lindex', KEYS[2], -1);" +
                            // 定义ttl变量
                    "local ttl;" +
                            // 条件一成立：说明lastThreadId不是空，有值
                            // 条件二成立：说明lastThreadId不等于当前要加锁的客户端的标识（说明当前客户端还没入队）
                    "if lastThreadId ~= false and lastThreadId ~= ARGV[2] then " +
                            // 获取lastThreadId的score值，并减去我们传入的currentTime。赋值给ttl
                        "ttl = tonumber(redis.call('zscore', KEYS[3], lastThreadId)) - tonumber(ARGV[4]);" +
                    "else " +
                            // 走到这，说明lastThreadId是空即等待队列为空 或者 等待队列不是空，但是lastThreadId就是当前请求加锁的客户端
                            // 直接获取anyLock的剩余过期时间
                        "ttl = redis.call('pttl', KEYS[1]);" +
                    "end;" +
                            // 为当前的客户端计算一个timeout：ttl + threadWaitTime + currentTime
                    "local timeout = ttl + tonumber(ARGV[3]) + tonumber(ARGV[4]);" +
                            // 条件成立：说明当前客户端和timeout值放入集合KEYS[3]成功
                    "if redis.call('zadd', KEYS[3], timeout, ARGV[2]) == 1 then " +
                            // 将当前客户端从等待队列KEYS[2]的右边入队
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

    // pexpire 命令用于设置 key 的过期时间，以毫秒计。key 过期后将不再可用
    @Override
    public RFuture<Boolean> expireAsync(long timeToLive, TimeUnit timeUnit) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "redis.call('pexpire', KEYS[2], ARGV[1]); " +
                        "return redis.call('pexpire', KEYS[3], ARGV[1]); ",
                Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName),
                timeUnit.toMillis(timeToLive));
    }

    // pexpireat命令用于设置 key 的过期时间，以毫秒计。key 过期后将不再可用
    @Override
    public RFuture<Boolean> expireAtAsync(long timestamp) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        "redis.call('pexpireat', KEYS[1], ARGV[1]); " +
                        "redis.call('pexpireat', KEYS[2], ARGV[1]); " +
                        "return redis.call('pexpireat', KEYS[3], ARGV[1]); ",
                Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName),
                timestamp);
    }

    // 执行persist移除锁的过期时间，这样锁就不会过期了
    @Override
    public RFuture<Boolean> clearExpireAsync() {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        "redis.call('persist', KEYS[1]); " +
                        "redis.call('persist', KEYS[2]); " +
                        "return redis.call('persist', KEYS[3]); ",
                Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName));
    }

    // 强制释放公平锁
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