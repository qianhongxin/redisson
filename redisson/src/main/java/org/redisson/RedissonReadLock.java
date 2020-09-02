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

                                // KEYS[1]：getName() hash结构的key，即我们设置的锁名称
                                // KEYS[2]：getReadWriteTimeoutNamePrefix(threadId)
                                // ARGV[1]：internalLockLeaseTime
                                // ARGV[2]：getLockName(threadId)
                                // ARGV[3]：getWriteLockName(threadId)

                                // 获取锁KEYS[1]中mode字段的值
                                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                                        // 条件成立：说明mode没有设置，即为nil。nil==false成立。说明此时还没有客户端来加过锁
                                "if (mode == false) then " +
                                        // 设置锁状态mode值为读锁，即read
                                  "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                                        // 将KEYS[1]的加锁客户端的v设置为1，表示加锁了
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                                        // lua的两个点语法“..”是指将前后两个字符串连接在一起
                                        // fixme
                                  "redis.call('set', KEYS[2] .. ':1', 1); " +
                                  "redis.call('pexpire', KEYS[2] .. ':1', ARGV[1]); " +
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                                  "return nil; " +
                                "end; " +

                                        // 条件一成立：说明加的是读锁
                                        // 条件二成立：说明加的是写锁，并且加写锁的客户端就是当前过来的客户端
                                "if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then " +
                                        // 如果ARGV[2]不存在就将ARGV[2]放入KEYS[1]中，存在就自增1
                                  "local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                                  "local key = KEYS[2] .. ':' .. ind;" +
                                  "redis.call('set', key, 1); " +
                                  "redis.call('pexpire', key, ARGV[1]); " +
                                  "local remainTime = redis.call('pttl', KEYS[1]); " +
                                  "redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1])); " +
                                  "return nil; " +
                                "end;" +

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
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == false) then " +
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                "local lockExists = redis.call('hexists', KEYS[1], ARGV[2]); " +
                "if (lockExists == 0) then " +
                    "return nil;" +
                "end; " +
                    
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " + 
                "if (counter == 0) then " +
                    "redis.call('hdel', KEYS[1], ARGV[2]); " + 
                "end;" +
                "redis.call('del', KEYS[3] .. ':' .. (counter+1)); " +
                
                "if (redis.call('hlen', KEYS[1]) > 1) then " +
                    "local maxRemainTime = -3; " + 
                    "local keys = redis.call('hkeys', KEYS[1]); " + 
                    "for n, key in ipairs(keys) do " + 
                        "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                        "if type(counter) == 'number' then " + 
                            "for i=counter, 1, -1 do " + 
                                "local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i); " + 
                                "maxRemainTime = math.max(remainTime, maxRemainTime);" + 
                            "end; " + 
                        "end; " + 
                    "end; " +
                            
                    "if maxRemainTime > 0 then " +
                        "redis.call('pexpire', KEYS[1], maxRemainTime); " +
                        "return 0; " +
                    "end;" + 
                        
                    "if mode == 'write' then " + 
                        "return 0;" + 
                    "end; " +
                "end; " +
                    
                "redis.call('del', KEYS[1]); " +
                "redis.call('publish', KEYS[2], ARGV[1]); " +
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
                "local counter = redis.call('hget', KEYS[1], ARGV[2]); " +
                "if (counter ~= false) then " +
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                    
                    "if (redis.call('hlen', KEYS[1]) > 1) then " +
                        "local keys = redis.call('hkeys', KEYS[1]); " + 
                        "for n, key in ipairs(keys) do " + 
                            "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                            "if type(counter) == 'number' then " + 
                                "for i=counter, 1, -1 do " + 
                                    "redis.call('pexpire', KEYS[2] .. ':' .. key .. ':rwlock_timeout:' .. i, ARGV[1]); " + 
                                "end; " + 
                            "end; " + 
                        "end; " +
                    "end; " +
                    
                    "return 1; " +
                "end; " +
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
