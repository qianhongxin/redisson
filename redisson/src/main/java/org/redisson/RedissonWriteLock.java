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
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

/**
 * Lock will be removed automatically if client disconnects.
 *
 * @author Nikita Koksharov
 *
 */
// 写锁,继承的RedissonLock即非公平
// 研究分布式锁的源码，一般来说不需要画图，就是之前最初的那个图就够了，后面的锁都是基于之前的最早最基础的那个锁的技术框架来实现的，然后redis的锁又是基于java的锁接口扩展的，后面的直接分析lua脚本

// 使用时是通过RedissonReadWriteLock使用，读写锁的name一样即KEYS[1]一样，共享一个锁数据结构
public class RedissonWriteLock extends RedissonLock implements RLock {

    protected RedissonWriteLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getName());
    }

    @Override
    protected String getLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }

    // 加写锁的具体lua脚本
    @Override
    <T> RFuture<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        internalLockLeaseTime = unit.toMillis(leaseTime);

        return evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                            // 注意读锁和写锁共用一个哈希结构的key的。和java一样，读写锁共用一个state值等
                            // 读写锁数据结构如下：
                            // anyLock:{
                            //      mode : write
                            //      UUID_01:threadId_01 : 2
                            // }

                            // KEYS[1]：getName()
                            // ARGV[1]: internalLockLeaseTime
                            // ARGV[2]: getLockName(threadId) （eg：UUID_01:threadId_01）

                            // 获取KEYS[1]中k为mode（锁类型）的值。如果没设置过返回nil，如果有返回对应的值
                            "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                                    // mode为nil时，说明还没有客户端对KEYS[1]加锁，所以这里mode==false成立
                            "if (mode == false) then " +
                                    // 设置KEYS[1]的mode值为write
                                  "redis.call('hset', KEYS[1], 'mode', 'write'); " +
                                    // 设置KEYS[1]的k为getLockName(threadId)的值为1
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                                    // 设置锁KEYS[1]的过期时间为internalLockLeaseTime
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                                    // 返回nil，表示加写锁成功
                                  "return nil; " +
                              "end; " +

                                    // 走到这，说明mode不是空，说明KEYS[1]写锁已经有客户端在持有了
                              "if (mode == 'write') then " +
                                    // 如果持有KEYS[1]锁的客户端是ARGV[2]对应的客户端，则执行可重入加锁逻辑。
                                  "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                                        // 将ARGV[2]的值自增1
                                      "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                                        // 获取剩余过期时间
                                      "local currentExpire = redis.call('pttl', KEYS[1]); " +
                                        // 将过期时间刷新为 currentExpire + internalLockLeaseTime
                                      "redis.call('pexpire', KEYS[1], currentExpire + ARGV[1]); " +
                                        // 返回nil，表名可重入加锁成功
                                      "return nil; " +
                                  "end; " +
                                "end;" +

                                    // 走到这，说明KEYS[1]锁已经被别的客户端持有，当前客户端加不了写/读锁，只能返回KEYS[1]的剩余过期时间回去
                                "return redis.call('pttl', KEYS[1]);",
                        Arrays.<Object>asList(getName()), 
                        internalLockLeaseTime, getLockName(threadId));
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // KEYS[1]：getName()
                // KEYS[2]: getChannelName()
                // ARGV[1]：LockPubSub.READ_UNLOCK_MESSAGE
                // ARGV[2]：internalLockLeaseTime
                // ARGV[3]：getLockName(threadId)

                // 获取KEYS[1]中k为mode（锁类型）的值。如果没设置过返回nil，如果有返回对应的值
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                        // mode为nil时，说明还没有客户端对KEYS[1]加锁，所以这里mode==false成立
                "if (mode == false) then " +
                        // 往pub/sub队列KEYS[2]中发布一个消息 LockPubSub.READ_UNLOCK_MESSAGE
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                        // 返回1，通知别的客户端可以来加锁了。且1标识着当前锁没有被任意客户端持有，空闲的
                    "return 1; " +
                "end;" +

                        // 条件成立：说明当前的锁状态是写锁
                "if (mode == 'write') then " +
                        // 判断锁KEYS[1]是否是客户端ARGV[3]加的
                    "local lockExists = redis.call('hexists', KEYS[1], ARGV[3]); " +
                        // 条件成立，说明锁KEYS[1]不是客户端ARGV[3]加的
                    "if (lockExists == 0) then " +
                        // 返回nil，表示当前写锁不是你加的，fuck，不是你加的你释放啥？表示unlock失败
                        "return nil;" +
                    "else " +
                        // 走到这，说明锁KEYS[1]是客户端ARGV[3]加的

                        // 将锁KEYS[1]的k为ARGV[3]的v扣减1（注意：这里的KEYS[1]是可重入的）。将扣减后的值返回设置到counter中
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        // 条件成立：说明ARGV[3]没有完全释放掉，即可重入加锁了
                        "if (counter > 0) then " +
                            // 重置锁的过期时间为internalLockLeaseTime
                            "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                            // 返回0，标识释放锁成功
                            "return 0; " +
                        "else " +
                            // 走到这，说明锁已经释放完了

                            // 删除KEYS[1]中的ARGV[3]了
                            "redis.call('hdel', KEYS[1], ARGV[3]); " +
                            // 条件成立：说明哈希表KEYS[1]中字段的数量不是空
                            "if (redis.call('hlen', KEYS[1]) == 1) then " +
                                // 直接删除锁KEYS[1]
                                "redis.call('del', KEYS[1]); " +
                                // 发布消息LockPubSub.READ_UNLOCK_MESSAGE到 pub/sub KEYS[2]
                                "redis.call('publish', KEYS[2], ARGV[1]); " + 
                            "else " +
                                // 走到这，说明哈希表KEYS[1]中字段的数量是空。直接将KEYS[1]的锁状态设置为read即读锁
                                // has unlocked read-locks
                                "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                            "end; " +
                            // 返回1，标识锁释放完毕，示意客户端可以过来加锁了
                            "return 1; "+
                        "end; " +
                    "end; " +
                "end; "
                        // 走到这，说明mode不是write，直接返回null，表示unlock失败
                + "return nil;",
        Arrays.<Object>asList(getName(), getChannelName()), 
        LockPubSub.READ_UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId));
    }
    
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    // 强制释放锁，任意客户端执行这个方法，都是直接删除锁，不论这个锁是不是他自己加的
    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
              "if (redis.call('hget', KEYS[1], 'mode') == 'write') then " +
                  "redis.call('del', KEYS[1]); " +
                  "redis.call('publish', KEYS[2], ARGV[1]); " +
                  "return 1; " +
              "end; " +
              "return 0; ",
              Arrays.<Object>asList(getName(), getChannelName()), LockPubSub.READ_UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getName(), StringCodec.INSTANCE, RedisCommands.HGET, getName(), "mode");
        String res = get(future);
        return "write".equals(res);
    }

}
