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
// 读锁,继承的RedissonLock即非公平
// 使用时是通过RedissonReadWriteLock使用，读写锁的name一样即KEYS[1]一样，共享一个锁数据结构
public class RedissonReadLock extends RedissonLock implements RLock {

    public RedissonReadLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getName());
    }
    
    String getWriteLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }

    String getReadWriteTimeoutNamePrefix(long threadId) {
        return suffixName(getName(), getLockName(threadId)) + ":rwlock_timeout"; 
    }

    // 异步加读锁lua脚本逻辑
    @Override
    <T> RFuture<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        internalLockLeaseTime = unit.toMillis(leaseTime);

        return evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                                // 注意读锁和写锁共用一个哈希结构的key的。和java一样，读写锁共用一个state值等
                                // 读写锁数据结构如下：
                                // anyLock:{
                                //      mode : read
                                //      UUID_01:threadId_01 : 2
                                //      UUID_02:threadId_02 : 1
                                //      ......
                                // }
                                // {anyLock}:UUID_01:threadId_01:rwlock_timeout:1 : 1
                                // {anyLock}:UUID_01:threadId_01:rwlock_timeout:2 : 1 （读锁每重入一次，这个可以就多生成一个，结尾的数字就是每次自增的数字，释放时也要依次删除）
                                // {anyLock}:UUID_02:threadId_02:rwlock_timeout:1 : 1

                                // KEYS[1]：getName() hash结构的key，即我们设置的锁名称。比如 anyLock
                                // KEYS[2]：getReadWriteTimeoutNamePrefix(threadId), 读写锁超时的key，比如 {anyLock}:UUID_01:threadId_01:rwlock_timeout
                                // ARGV[1]：internalLockLeaseTime, 比如默认的 30s
                                // ARGV[2]：getLockName(threadId)，uuid+线程id，比如 UUID_01:threadId_01
                                // ARGV[3]：getWriteLockName(threadId)，写锁的key，比如 UUID_01:threadId_01:write

                                // 获取锁KEYS[1]中mode字段的值
                                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                                        // 条件成立：说明mode没有设置，即为nil。nil==false成立。说明此时还没有客户端来加过锁
                                "if (mode == false) then " +
                                        // 设置锁状态mode值为读锁，即read
                                  "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                                        // 给anyLock添加k-v：UUID_01:threadId_01 1表示加读锁了
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                                        // lua的两个点语法“..”是指将前后两个字符串连接在一起
                                        // 新增 key：{anyLock}:UUID_01:threadId_01:rwlock_timeout:1，value是1
                                  "redis.call('set', KEYS[2] .. ':1', 1); " +
                                        // 设置 {anyLock}:UUID_01:threadId_01:rwlock_timeout:1 的过期时间为 internalLockLeaseTime
                                  "redis.call('pexpire', KEYS[2] .. ':1', ARGV[1]); " +
                                        // 设置anyLock的过期时间为 internalLockLeaseTime
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                                        // 加读锁成功，返回nil
                                  "return nil; " +
                                "end; " +

                                        // 条件一成立：说明加的是读锁
                                        // 条件二成立：说明加的是写锁，并且加写锁的客户端就是当前过来的客户端
                                        // 所以：如果当前锁模式是读锁，任意想要加读锁的客户端都可以加读锁；如果当前锁模式是写锁并且就是当前客户端加的写锁就可以进入当前if
                                        //       同一个客户端同一个线程，先读锁再写锁，是互斥的，会导致加锁失败
                                        //        (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1)：writeLock.lock()，然后readLock.lock()是可以的。同一个客户端同一个线程先加写锁的，然后加读锁也是可以成功的，因为读锁这里支持了这种情况
                                        //              之前的先加写锁结构：anyLock: {
                                        //                              “mode”: “write”,
                                        //                              “UUID_01:threadId_01:write”: 1
                                        //                           }
                                        //              现在又加读锁后的结构变为：anyLock: {
                                        //                                  “mode”: “write”,
                                        //                                  “UUID_01:threadId_01:write”: 1,
                                        //                                  “UUID_01:threadId_01”: 1
                                        //                                }
                                        //                                {anyLock}:UUID_01:threadId_01:rwlock_timeout:1		1
                                "if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then " +
                                        // 如果ARGV[2]不存在就将ARGV[2]放入KEYS[1]中，存在就自增1
                                        // 将anyLock的UUID_01:threadId_01的值自增1，不存在就设置进去为1
                                  "local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                                        // 如果是自增1，为2。就是创建一个新的key：{anyLock}:UUID_01:threadId_01:rwlock_timeout:2
                                  "local key = KEYS[2] .. ':' .. ind;" +
                                        // 设置新的key：{anyLock}:UUID_01:threadId_01:rwlock_timeout:2 1
                                  "redis.call('set', key, 1); " +
                                        // 将{anyLock}:UUID_01:threadId_01:rwlock_timeout:ind的过期时间设置为 internalLockLeaseTime
                                  "redis.call('pexpire', key, ARGV[1]); " +
                                        // 获取anyLock的剩余过期时间
                                  "local remainTime = redis.call('pttl', KEYS[1]); " +
                                        // 从remainTime和internalLockLeaseTime中取一个最大的时间设置成anyLock的过期时间
                                  "redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1])); " +
                                        // 返回客户端nil，表示可重入加锁成功
                                  "return nil; " +
                                "end;" +

                                        // 什么情况走到这？
                                        // 当前锁模型是写锁，且当前UUID_01:threadId_01:write的值不是当前客户端，即加写锁失败
                                        // 返回锁anyLock的过期时间
                                "return redis.call('pttl', KEYS[1]);",
                        Arrays.<Object>asList(getName(), getReadWriteTimeoutNamePrefix(threadId)), 
                        internalLockLeaseTime, getLockName(threadId), getWriteLockName(threadId));
    }

    // 异步释放读锁lua脚本逻辑
    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);

        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // KEYS[1]：getName()，比如 anyLock
                // KEYS[2]：getChannelName(),比如redisson_rwlock:{anyLock}
                // KEYS[3]：timeoutPrefix 比如 {anyLock}:UUID_01:threadId_01:rwlock_timeout
                // KEYS[4]：keyPrefix，比如{anyLock}
                // ARGV[1]：LockPubSub.UNLOCK_MESSAGE
                // ARGV[2]：getLockName(threadId)，比如 UUID_01:threadId_01


                        // 获取anyLock的mode值
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                        // mode == false表示当前没有客户端来加锁，即锁已经释放完毕了，直接发布消息到pub/sub中
                "if (mode == false) then " +
                        // 发布LockPubSub.UNLOCK_MESSAGE到getChannelName()中
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                        // 返回1，表示当前没有加锁，空闲的，不用释放
                    "return 1; " +
                "end; " +

                        // 判断anyLock中是否存在 key为UUID_01:threadId_01的客户端标识
                "local lockExists = redis.call('hexists', KEYS[1], ARGV[2]); " +
                        // 为0，说明当前客户端没有占用读锁
                "if (lockExists == 0) then " +
                        // 返回nil
                    "return nil;" +
                "end; " +

                        // 走到这，说明当前客户端 UUID_01:threadId_01 加过读锁，这里做 -1 操作
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " +
                        // counter为0，说明读锁释放完毕，删除 anyLock 中的 UUID_01:threadId_01 客户端标识
                "if (counter == 0) then " +
                        // 读锁释放完毕，删除 anyLock 中的 UUID_01:threadId_01 客户端标识
                    "redis.call('hdel', KEYS[1], ARGV[2]); " + 
                "end;" +
                        // 走到这，说明读锁释放过了一次，删除读锁对应的timeoutkey，比如{anyLock}:UUID_01:threadId_01:rwlock_timeout:2
                "redis.call('del', KEYS[3] .. ':' .. (counter+1)); " +
                        // 条件成立：说明anyLock中还有值，即还有没释放的锁
                "if (redis.call('hlen', KEYS[1]) > 1) then " +
                    "local maxRemainTime = -3; " +
                        // 获取anyLock中的所有的key
                    "local keys = redis.call('hkeys', KEYS[1]); " +
                        // 遍历keys
                    "for n, key in ipairs(keys) do " +
                        // 获取key的value
                        "counter = tonumber(redis.call('hget', KEYS[1], key)); " +
                        // 条件成立：说明value是数字类型，即对应的key是表示锁的，即不是mode这种
                        "if type(counter) == 'number' then " +
                            // 遍历counter，即将counter依次减一直到1执行for的逻辑。比如counter是5，则一次执行5,4,3,2,1
                            "for i=counter, 1, -1 do " +
                                // 返回{anyLock}:UUID_01:threadId_01:rwlock_timeout:i的剩余过期时间
                                "local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i); " + 
                                // 获取一个最大的时间值
                                "maxRemainTime = math.max(remainTime, maxRemainTime);" +
                            "end; " + 
                        "end; " + 
                    "end; " +

                        // 最大时间值如果大于0
                    "if maxRemainTime > 0 then " +
                        // 将这个时间设置为anyLock的过期时间
                        "redis.call('pexpire', KEYS[1], maxRemainTime); " +
                        // 返回0
                        "return 0; " +
                    "end;" + 
                        // 条件成立：说明锁类型是写锁。上面的释放锁逻辑也适用于写锁的释放，可以看
                    "if mode == 'write' then " +
                        // 直接返回0
                        "return 0;" + 
                    "end; " +
                "end; " +

                        // 走到这，说明anyLock的所有读锁已经释放完毕了，这里直接删除anyLock
                "redis.call('del', KEYS[1]); " +
                        // 发布 LockPubSub.UNLOCK_MESSAGE 到 getChannelName() 中
                "redis.call('publish', KEYS[2], ARGV[1]); " +
                        // 返回1，告诉客户端已经释放完毕了
                "return 1; ",
                Arrays.<Object>asList(getName(), getChannelName(), timeoutPrefix, keyPrefix), 
                LockPubSub.UNLOCK_MESSAGE, getLockName(threadId));
    }

    protected String getKeyPrefix(long threadId, String timeoutPrefix) {
        return timeoutPrefix.split(":" + getLockName(threadId))[0];
    }

    // 看门狗异步给读锁续期的lua脚本逻辑
    @Override
    protected RFuture<Boolean> renewExpirationAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);
        
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // KEYS[1]：getName() 比如 anyLock
                // KEYS[2]：keyPrefix
                // ARGV[1]：internalLockLeaseTime 比如30s
                // ARGV[2]：getLockName(threadId) 比如 UUID_01:threadId_01

                // 获取锁anyLock的客户端UUID_01:threadId_01的值
                "local counter = redis.call('hget', KEYS[1], ARGV[2]); " +
                        // 条件成立：说明当前客户端已经加过锁了
                "if (counter ~= false) then " +
                        // 刷新anyLock的过期时间为 internalLockLeaseTime
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +

                        // 条件成立：说明anyLock中存在加锁客户端（为啥大于1，因为有一个key是mode。已经占了一个坑了）
                    "if (redis.call('hlen', KEYS[1]) > 1) then " +
                        // 拿到anyLock中的所有的key
                        "local keys = redis.call('hkeys', KEYS[1]); " +
                        // 遍历所有的key
                        "for n, key in ipairs(keys) do " +
                            // 拿到anyLock中指定key的值
                            "counter = tonumber(redis.call('hget', KEYS[1], key)); " +
                            // 条件成立：说明counter是数字类型，即key是个加锁客户端标识比如 UUID_01:threadId_01
                            "if type(counter) == 'number' then " +
                                // 遍历counter，即将counter依次减一执行for的逻辑。比如counter是5，则一次执行5,4,3,2,1
                                "for i=counter, 1, -1 do " +
                                    // 刷新{anyLock}:UUID_01:threadId_01:rwlock_timeout:i的剩余过期时间为 internalLockLeaseTime
                                    "redis.call('pexpire', KEYS[2] .. ':' .. key .. ':rwlock_timeout:' .. i, ARGV[1]); " + 
                                "end; " + 
                            "end; " + 
                        "end; " +
                    "end; " +
                        // 走到这说明锁的过期时间刷新成功，返回1
                    "return 1; " +
                "end; " +
                        // 走到这，说明当前客户端没有加过锁，或者锁释放了，直接返回0，不用刷新过期时间
                "return 0;",
            Arrays.<Object>asList(getName(), keyPrefix), 
            internalLockLeaseTime, getLockName(threadId));
    }
    
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    // 强制删除锁，直接删除，不论是否有客户端持有锁
    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hget', KEYS[1], 'mode') == 'read') then " +
                    "redis.call('del', KEYS[1]); " +
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                "return 0; ",
                Arrays.<Object>asList(getName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getName(), StringCodec.INSTANCE, RedisCommands.HGET, getName(), "mode");
        String res = get(future);
        return "read".equals(res);
    }

}
