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

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.redisson.api.BatchOptions;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.RedisException;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommand.ValueType;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.client.protocol.convertor.IntegerReplayConvertor;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandBatchService;
import org.redisson.connection.MasterSlaveEntry;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;
import org.redisson.pubsub.LockPubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p>
 * Implements a <b>non-fair</b> locking so doesn't guarantees an acquire order.
 *
 * @author Nikita Koksharov
 *
 */
// 该类中的是非公平的可重入锁实现，即胡乱的争抢，毫无公平可言。子类有公平锁，读写锁等实现
public class RedissonLock extends RedissonExpirable implements RLock {

    public static class ExpirationEntry {
        
        private final Map<Long, Integer> threadIds = new LinkedHashMap<>();
        private volatile Timeout timeout;
        
        public ExpirationEntry() {
            super();
        }
        
        public void addThreadId(long threadId) {
            Integer counter = threadIds.get(threadId);
            if (counter == null) {
                counter = 1;
            } else {
                counter++;
            }
            threadIds.put(threadId, counter);
        }
        public boolean hasNoThreads() {
            return threadIds.isEmpty();
        }
        public Long getFirstThreadId() {
            if (threadIds.isEmpty()) {
                return null;
            }
            return threadIds.keySet().iterator().next();
        }

        public void removeThreadId(long threadId) {
            // 因为锁是可重入的，所以这里先拿到锁对应的加锁次数
            Integer counter = threadIds.get(threadId);
            if (counter == null) {
                return;
            }
            // 自减
            counter--;
            // 条件成立，说明锁释放完毕，直接remove掉threadId
            // 条件不成立，直接更新threadId的counter值
            if (counter == 0) {
                threadIds.remove(threadId);
            } else {
                threadIds.put(threadId, counter);
            }
        }
        
        
        public void setTimeout(Timeout timeout) {
            this.timeout = timeout;
        }
        public Timeout getTimeout() {
            return timeout;
        }
        
    }
    
    private static final Logger log = LoggerFactory.getLogger(RedissonLock.class);

    private static final ConcurrentMap<String, ExpirationEntry> EXPIRATION_RENEWAL_MAP = new ConcurrentHashMap<>();
    protected long internalLockLeaseTime;

    final String id;
    final String entryName;

    protected final LockPubSub pubSub;

    final CommandAsyncExecutor commandExecutor;

    // name使我们传入的lock name
    public RedissonLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.id = commandExecutor.getConnectionManager().getId();
        // 锁过期时间，默认30s
        this.internalLockLeaseTime = commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout();
        this.entryName = id + ":" + name;
        this.pubSub = commandExecutor.getConnectionManager().getSubscribeService().getLockPubSub();
    }

    protected String getEntryName() {
        return entryName;
    }

    String getChannelName() {
        return prefixName("redisson_lock__channel", getName());
    }

    protected String getLockName(long threadId) {
        return id + ":" + threadId;
    }

    // 不支持过期时间且不支持中断的加锁
    @Override
    public void lock() {
        try {
            lock(-1, null, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }

    // 支持过期时间但不支持中断的加锁
    // 支持设置续约时间，如果设置了不是-1就不会启动看门狗
    @Override
    public void lock(long leaseTime, TimeUnit unit) {
        try {
            lock(leaseTime, unit, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }


    // 支持中断的加锁
    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock(-1, null, true);
    }

    // 支持中断和过期时间的加锁
    @Override
    public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
        lock(leaseTime, unit, true);
    }

    // 加锁成功，直接返回，失败要自旋等待不断尝试获取锁直到成功
    private void lock(long leaseTime, TimeUnit unit, boolean interruptibly) throws InterruptedException {
        // 拿到当前线程id
        long threadId = Thread.currentThread().getId();
        // 尝试加锁
        Long ttl = tryAcquire(leaseTime, unit, threadId);
        // lock acquired
        // 条件成立：加锁成功
        if (ttl == null) {
            return;
        }

        // 加锁失败处理
        RFuture<RedissonLockEntry> future = subscribe(threadId);
        if (interruptibly) {
            commandExecutor.syncSubscriptionInterrupted(future);
        } else {
            commandExecutor.syncSubscription(future);
        }

        try {
            // 自旋
            // 尝试加锁，直到成功
            while (true) {
                // 尝试加锁，进到lua脚本那里分析知道，如果加锁成功ttl是null，加锁失败ttl是当前锁剩余存活时间，那后面只要
                // 等待ttl的时间后再去获取锁就行了
                ttl = tryAcquire(leaseTime, unit, threadId);
                // lock acquired
                // 加锁成功，直接退出循环
                if (ttl == null) {
                    break;
                }

                // waiting for message
                // 这两行代码的细节，不用过多的关注，里面涉及到了其他的同步组件，Semaphore，在这里的话呢，如果获取锁不成功，此时就会等待一段时间，再次投入到while(true)死循环的逻辑内，尝试去获取锁
                //
                //以此循环往复
                //
                //看到这样的一个分布式锁的阻塞逻辑，如果一个客户端的其他线程，或者是其他客户端的线程，尝试获取一个已经被加锁的key的锁，就会在while(true)死循环里被无限制的阻塞住，无限制的等待，尝试获取这把锁
                if (ttl >= 0) {
                    try {
                        // 利用信号量来做的等待
                        future.getNow().getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        // 抛出中断异常时，如果interruptibly为true，则表示当前逻辑响应中断，直接抛出中断异常即可，表示被中断了
                        if (interruptibly) {
                            throw e;
                        }
                        // 走到这，表示不响应中断
                        future.getNow().getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                    }
                } else {
                    if (interruptibly) {
                        future.getNow().getLatch().acquire();
                    } else {
                        future.getNow().getLatch().acquireUninterruptibly();
                    }
                }
            }
        } finally {
            unsubscribe(future, threadId);
        }
//        get(lockAsync(leaseTime, unit));
    }
    
    private Long tryAcquire(long leaseTime, TimeUnit unit, long threadId) {
        // tryAcquireAsync(leaseTime, unit, threadId)返回值是个future。所以这里get获取返回值
        return get(tryAcquireAsync(leaseTime, unit, threadId));
    }

    // 只做一次获取锁的操作，不论成功或失败直接返回
    private RFuture<Boolean> tryAcquireOnceAsync(long leaseTime, TimeUnit unit, long threadId) {
        if (leaseTime != -1) {
            // leaseTime：锁的过期时间
            // 如果走这里，说明看门狗就不会生效，所以注意调用的方法。比如调用tryAcquireOnceAsync的方法传入的leaseTime不是-1，则这里就加完锁直接返回了，就不会启动看门狗了

            // 如果你自己指定了一个leaseTime，就会直接执行lua脚本去加锁，加完锁的结果就直接返回了，并不会对那个future加一个监听器（看门狗）以及执行定时调度任务
            // 去刷新key的生存周期，因为你已经指定了leaseTime以后，就意味着你需要的是这个key最多存在10秒钟，必须被删除

            // 也就是说，人家在加锁的时候就设定好了，我们的锁key最多就只能存活10秒钟，而且后台没有定时调度的任务不断的去刷新锁key的生存周期
            // 我们的那个锁到了10秒钟，就会自动被redis给删除，生存时间只能是10秒钟，然后就会自动释放掉了，别的客户端就可以加锁了，但是在10秒之内，
            // 其实你也可以自己去手动释放锁
            return tryLockInnerAsync(leaseTime, unit, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        }
        // 启动看门狗续约，默认过期时间是30s，就定时30/3=10s来续约
        RFuture<Boolean> ttlRemainingFuture = tryLockInnerAsync(commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout(), TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        ttlRemainingFuture.onComplete((ttlRemaining, e) -> {
            if (e != null) {
                return;
            }

            // lock acquired
            // 加锁成功，执行看门狗续约逻辑
            if (ttlRemaining) {
                scheduleExpirationRenewal(threadId);
            }
        });
        return ttlRemainingFuture;
    }

    // 尝试异步获取锁，即异步执行redis加锁命令
    private <T> RFuture<Long> tryAcquireAsync(long leaseTime, TimeUnit unit, long threadId) {
        // 条件成立，说明我们传的leaseTime不为空
        if (leaseTime != -1) {
            // leaseTime：锁的过期时间
            // 如果走这里，说明看门狗就不会生效，所以注意调用的方法。比如调用tryAcquireAsync的方法传入的leaseTime不是-1，则这里就加完锁直接返回了，就不会启动看门狗了

            // 如果你自己指定了一个leaseTime，就会直接执行lua脚本去加锁，加完锁的结果就直接返回了，并不会对那个future加一个监听器（看门狗）以及执行定时调度任务
            // 去刷新key的生存周期，因为你已经指定了leaseTime以后，就意味着你需要的是这个key最多存在10秒钟，必须被删除

            // 也就是说，人家在加锁的时候就设定好了，我们的锁key最多就只能存活10秒钟，而且后台没有定时调度的任务不断的去刷新锁key的生存周期
            // 我们的那个锁到了10秒钟，就会自动被redis给删除，生存时间只能是10秒钟，然后就会自动释放掉了，别的客户端就可以加锁了，但是在10秒之内，
            // 其实你也可以自己去手动释放锁
            return tryLockInnerAsync(leaseTime, unit, threadId, RedisCommands.EVAL_LONG);
        }
        // commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout()  默认释放锁时间，默认30秒（看门狗也是30s释放）
        // 执行加锁逻辑，锁默认是30s（commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout()=30s）过期
        RFuture<Long> ttlRemainingFuture = tryLockInnerAsync(commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout(), TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_LONG);
        // ttlRemainingFuture封装的是当前的key对应的剩余的存活时间，单位是ms
        ttlRemainingFuture.onComplete((ttlRemaining, e) -> {
            if (e != null) {
                return;
            }

            // lock acquired
            // 条件成立：说明tryLockInnerAsync执行的lua脚本加锁成功了，这里执行看门狗逻辑，即启动定时任务，定时刷新锁的过期时间
            if (ttlRemaining == null) {
                scheduleExpirationRenewal(threadId);
            }
        });
        return ttlRemainingFuture;
    }

    // 和lock不同的是，这里是做一次获取锁的逻辑，获取失败不再获取，也不等待
    @Override
    public boolean tryLock() {
        // 利用get，将异步转同步，即等待结果
        return get(tryLockAsync());
    }

    // watch dog执行的续约锁时间的逻辑
    private void renewExpiration() {
        // 从EXPIRATION_RENEWAL_MAP中获取我们加的锁对应的name的ExpirationEntry
        ExpirationEntry ee = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        // ee == null =》说明锁已经被释放了，这里直接返回，结束当前锁对应的看门狗定时任务即可
        if (ee == null) {
            return;
        }

        // 创建续约任务
        Timeout task = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                ExpirationEntry ent = EXPIRATION_RENEWAL_MAP.get(getEntryName());
                // ee == null =》说明锁已经被释放了，这里直接返回，结束当前task
                if (ent == null) {
                    return;
                }
                // 返回ent中第一个加锁线程id
                Long threadId = ent.getFirstThreadId();
                // threadId == null =》说明锁已经被释放了，这里直接返回，结束当前task
                if (threadId == null) {
                    return;
                }

                // 为 threadId 执行异步续约逻辑
                RFuture<Boolean> future = renewExpirationAsync(threadId);
                future.onComplete((res, e) -> {
                    if (e != null) {
                        log.error("Can't update lock " + getName() + " expiration", e);
                        return;
                    }

                    // 续约成功返回1，这里的res对应的是false
                    // 续约失败返回0，这里的res对应的是true
                    // res为true，说明当前看门狗续约失败，重新执行续约逻辑
                    if (res) {
                        // reschedule itself
                        // 续约失败时，这里重新新建一个续约任务，丢弃当前的task
                        renewExpiration();
                    }
                });
            }
            // internalLockLeaseTime/3是血续约间隔时间，单位是TimeUnit.MILLISECONDS，比如3000/3=1000，即每秒续约一次
            // 续约逻辑：比如我们设置的leaseTime=30s，表示给redis上锁的过期时间设置为30s。这里的看门狗续约是指每10s，如果没有释放锁
                        // 看门狗通过lua脚本给锁续约重新变为30s。如果当前进程宕机，或者gc导致超时，redis的锁都会30s后自动释放的
        }, internalLockLeaseTime / 3, TimeUnit.MILLISECONDS);

        // 将task设置到ee中
        ee.setTimeout(task);
    }
    
    private void scheduleExpirationRenewal(long threadId) {
        // 创建一个过期实体
        ExpirationEntry entry = new ExpirationEntry();
        // 加入EXPIRATION_RENEWAL_MAP
        ExpirationEntry oldEntry = EXPIRATION_RENEWAL_MAP.putIfAbsent(getEntryName(), entry);
        if (oldEntry != null) {
            // EXPIRATION_RENEWAL_MAP维护的就是锁名称和线程id的关系
            // oldEntry不是null，说明getEntryName()在本机已经有线程加过锁了，这里再次addThreadId，会给线程的对应的count自增，实现可重入效果
            // 同一个时间，getEntryName()，只会有一个线程加锁成功的
            oldEntry.addThreadId(threadId);
        } else {
            // 添加一个新的threadId到entry中，说明是新的执行体对getEntryName()获取锁
            entry.addThreadId(threadId);
            // 执行续约逻辑
            renewExpiration();
        }
    }

    protected RFuture<Boolean> renewExpirationAsync(long threadId) {
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // 续约成功返回1，否则返回0
                "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return 1; " +
                        "end; " +
                        "return 0;",
                Collections.singletonList(getName()),
                internalLockLeaseTime, getLockName(threadId));
    }

    // 释放锁时会调用这个方法从EXPIRATION_RENEWAL_MAP中删除对应的执行体对象，取消看门狗定时任务对他的续约即刷新过期时间
    void cancelExpirationRenewal(Long threadId) {
        ExpirationEntry task = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (task == null) {
            return;
        }
        
        if (threadId != null) {
            // 执行本地可重入锁的释放锁逻辑
            task.removeThreadId(threadId);
        }

        // 条件一成立：说明task也为空，上面判断了，直接从EXPIRATION_RENEWAL_MAP移除
        // 条件二成立：说明当前的task中没有加锁线程了，直接从EXPIRATION_RENEWAL_MAP移除
        if (threadId == null || task.hasNoThreads()) {
            Timeout timeout = task.getTimeout();
            if (timeout != null) {
                timeout.cancel();
            }
            // 删除锁，取消getEntryName()的看门狗的续约
            EXPIRATION_RENEWAL_MAP.remove(getEntryName());
        }
    }

    // 这里会被加锁/释放锁/续约锁调用，传入lua脚本等参数
    protected <T> RFuture<T> evalWriteAsync(String key, Codec codec, RedisCommand<T> evalCommandType, String script, List<Object> keys, Object... params) {
        CommandBatchService executorService = createCommandBatchService();
        RFuture<T> result = executorService.evalWriteAsync(key, codec, evalCommandType, script, keys, params);
        if (!(commandExecutor instanceof CommandBatchService)) {
            executorService.executeAsync();
        }
        return result;
    }

    // redis上的锁是hset结构，hset的name是我们设置的name，即getName()值；值=》key：随机字符串（uuid+线程id），value是数字1（2等）；过期时间：3000ms
    // keys是：Collections.singletonList(getName())
    // params是：internalLockLeaseTime 和 getLockName(threadId)
    <T> RFuture<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        // 锁过期时间
        internalLockLeaseTime = unit.toMillis(leaseTime);

        //-------参数解释如下-------
        // KEYS[1]是 getName()，我们在getLock（name）时指定的name值
        // ARGV[1]是 internalLockLeaseTime
        // ARGV[2]是 getLockName(threadId)


        // 执行lua脚本，针对redis加锁的一段命令。redis单线程执行命令的，所以执行lua是原子的
        // 如果KEYS[1]不存在
        //if (redis.call('exists', KEYS[1]) == 0) then " +
        // 将KEYS[1]这个key对应的ARGV[2]参数设置1
        //"redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
        // 将KEYS[1]的有效期设为ARGV[1]
        //"redis.call('pexpire', KEYS[1], ARGV[1]); " +
        //"return nil; " +
        //"end; " +

        // 如果KEYS[1]存在
        //"if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
        // 将KEYS[1]这个key对应的ARGV[2]参数累加1
        // 这里既是可重入锁的概念了，表示可重入锁
        //"redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
        // 再次将KEYS[1]的有效期设为ARGV[1]
        // 整个KEYS[1]的生存周期重新刷新为ARGV[1]比如30000毫秒，pexpire KEYS[1] 30000
        //"redis.call('pexpire', KEYS[1], ARGV[1]); " +
        //"return nil; " +
        //"end; " +

        // 上面两个if都不成功，走这里即返回KEYS[1]剩余存活时间
        // 如果上面两个if都不执行，则执行这个pttl指令，返回KEYS[1]的剩余存活时间，单位是ms。当前lua脚本执行完后就返回这个pttl执行结果
        // 这个时候返回的不是null，因为上面两个if判断，锁都不存在，也就是加锁失败（说明是别的执行体想过来加锁），调用时feture.getNow()就不是null，看门狗就不会继续下去了。见258行
        //"return redis.call('pttl', KEYS[1]);


        //-------以上lua脚本返回结果解释如下-------
        // 第一个if成功说明第一次加锁成功，返回null
        // 第二个if成功说明可重入加锁成功，返回null
        // 走了第三个return，说明加锁失败，返回的是当前key的剩余存活时间
        // 总结：当前加锁的lua脚本如果返回null，说明加锁/可重入加锁成功。返回剩余时间，说明加锁失败，根据调用需要是否执行等待逻辑


        //-------一些说明-------
        // 这里锁的数据结构用的哈希结构，key就是我们获取锁时指定的name。value是个k-v结构，k是redisson生成的，利用uuid+线程id生成，v是整数 从1开始自增来支持可重入
        // 这里可重入锁加锁为啥要用lua脚本，而不是简单的set nx px命令？
             // 减少网络开销：本来多次网络请求的操作，可以用一个请求完成，原先多次请求的逻辑放在redis服务器上完成。使用脚本，减少了网络往返时延
             // 原子操作：Redis会将整个脚本作为一个整体执行，中间不会被其他命令插入
            //  复用：客户端发送的脚本会永久存储在Redis中，意味着其他客户端可以复用这一脚本而不需要使用代码完成同样的逻辑
             // 主要原因：简单的set nx px命令并不支持可重入锁，这里利用哈希结构，实现k是随机值来达到set nx px命令的效果，然后v是数字来支持可重入的效果
             //         还有就是这个哈希结构的key是getName()，任意客户端加任意锁在redis中都是存储在这个对应的getName()为名字的哈希结构中，
             //         value就是各个线程加的锁即k-v结构，k是随机值即getLockName(threadId)防止误删除，v是整数。redis用哈希结构管理同一个name的所有执行体的加锁，而哈希不支持
             //         多个值的set nx px的效果，只能用lua来实现多个指令的原子性
        return evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                "if (redis.call('exists', KEYS[1]) == 0) then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return nil; " +
                        "end; " +
                        "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return nil; " +
                        "end; " +
                        "return redis.call('pttl', KEYS[1]);",
                Collections.singletonList(getName()), internalLockLeaseTime, getLockName(threadId));
    }

    private CommandBatchService createCommandBatchService() {
        if (commandExecutor instanceof CommandBatchService) {
            return (CommandBatchService) commandExecutor;
        }

        MasterSlaveEntry entry = commandExecutor.getConnectionManager().getEntry(getName());
        BatchOptions options = BatchOptions.defaults()
                                .syncSlaves(entry.getAvailableSlaves(), 1, TimeUnit.SECONDS);

        return new CommandBatchService(commandExecutor.getConnectionManager(), options);
    }

    private void acquireFailed(long threadId) {
        get(acquireFailedAsync(threadId));
    }
    
    protected RFuture<Void> acquireFailedAsync(long threadId) {
        return RedissonPromise.newSucceededFuture(null);
    }

    // 支持获取锁超时，支持设置锁过期时间，超时自动释放（如果看门狗没设置），leaseTime不传默认是30s
    @Override
    public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
        long time = unit.toMillis(waitTime);
        long current = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        Long ttl = tryAcquire(leaseTime, unit, threadId);
        // lock acquired
        if (ttl == null) {
            return true;
        }

        // 将time减去耗时
        time -= System.currentTimeMillis() - current;
        // time小于等于0成立，说明超时了，加锁失败，直接返回false
        if (time <= 0) {
            acquireFailed(threadId);
            return false;
        }


        // 这边在等待一些时间，基于pub/sub做一些事情
        current = System.currentTimeMillis();
        RFuture<RedissonLockEntry> subscribeFuture = subscribe(threadId);
        if (!subscribeFuture.await(time, TimeUnit.MILLISECONDS)) {
            if (!subscribeFuture.cancel(false)) {
                subscribeFuture.onComplete((res, e) -> {
                    if (e == null) {
                        unsubscribe(subscribeFuture, threadId);
                    }
                });
            }
            acquireFailed(threadId);
            return false;
        }

        try {
            // time再次减去耗时
            time -= System.currentTimeMillis() - current;
            if (time <= 0) {
                // 加锁失败
                acquireFailed(threadId);
                return false;
            }

            // 接下来进入死循环，不断的尝试获取锁、等待，每次time都不断的减去尝试获取锁的耗时，
            // 以及等待的耗时，然后如果说在time范围内，获取到了锁，就会返回true，如果始终无法
            // 获取到锁的话，那么就会在time指定的最大时间之后，就返回一个false
            while (true) {
                long currentTime = System.currentTimeMillis();
                ttl = tryAcquire(leaseTime, unit, threadId);
                // lock acquired
                if (ttl == null) {
                    return true;
                }

                time -= System.currentTimeMillis() - currentTime;
                if (time <= 0) {
                    acquireFailed(threadId);
                    return false;
                }

                // waiting for message
                currentTime = System.currentTimeMillis();
                if (ttl >= 0 && ttl < time) {
                    subscribeFuture.getNow().getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    subscribeFuture.getNow().getLatch().tryAcquire(time, TimeUnit.MILLISECONDS);
                }

                time -= System.currentTimeMillis() - currentTime;
                if (time <= 0) {
                    acquireFailed(threadId);
                    return false;
                }
            }
        } finally {
            unsubscribe(subscribeFuture, threadId);
        }
//        return get(tryLockAsync(waitTime, leaseTime, unit));
    }

    protected RFuture<RedissonLockEntry> subscribe(long threadId) {
        return pubSub.subscribe(getEntryName(), getChannelName());
    }

    protected void unsubscribe(RFuture<RedissonLockEntry> future, long threadId) {
        pubSub.unsubscribe(future.getNow(), getEntryName(), getChannelName());
    }

    // 同步获取锁，只获取一次，获取成功时30s后自动释放
    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        return tryLock(waitTime, -1, unit);
    }

    // 释放锁逻辑，并且获取异步释放锁的结果
    @Override
    public void unlock() {
        try {
            get(unlockAsync(Thread.currentThread().getId()));
        } catch (RedisException e) {
            if (e.getCause() instanceof IllegalMonitorStateException) {
                throw (IllegalMonitorStateException) e.getCause();
            } else {
                throw e;
            }
        }
        
//        Future<Void> future = unlockAsync();
//        future.awaitUninterruptibly();
//        if (future.isSuccess()) {
//            return;
//        }
//        if (future.cause() instanceof IllegalMonitorStateException) {
//            throw (IllegalMonitorStateException)future.cause();
//        }
//        throw commandExecutor.convertException(future);
    }

    @Override
    public Condition newCondition() {
        // TODO implement
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forceUnlock() {
        return get(forceUnlockAsync());
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('del', KEYS[1]) == 1) then "
                        + "redis.call('publish', KEYS[2], ARGV[1]); "
                        + "return 1 "
                        + "else "
                        + "return 0 "
                        + "end",
                Arrays.asList(getName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        return isExists();
    }
    
    @Override
    public RFuture<Boolean> isLockedAsync() {
        return isExistsAsync();
    }

    @Override
    public RFuture<Boolean> isExistsAsync() {
        return commandExecutor.writeAsync(getName(), codec, RedisCommands.EXISTS, getName());
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return isHeldByThread(Thread.currentThread().getId());
    }

    @Override
    public boolean isHeldByThread(long threadId) {
        RFuture<Boolean> future = commandExecutor.writeAsync(getName(), LongCodec.INSTANCE, RedisCommands.HEXISTS, getName(), getLockName(threadId));
        return get(future);
    }

    private static final RedisCommand<Integer> HGET = new RedisCommand<Integer>("HGET", ValueType.MAP_VALUE, new IntegerReplayConvertor(0));
    
    public RFuture<Integer> getHoldCountAsync() {
        return commandExecutor.writeAsync(getName(), LongCodec.INSTANCE, HGET, getName(), getLockName(Thread.currentThread().getId()));
    }
    
    @Override
    public int getHoldCount() {
        return get(getHoldCountAsync());
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return forceUnlockAsync();
    }

    // 异步释放锁
    @Override
    public RFuture<Void> unlockAsync() {
        long threadId = Thread.currentThread().getId();
        return unlockAsync(threadId);
    }

    // 这里是手动调用释放锁逻辑
    // 如果宕机来，宕机自动释放锁，这个锁对应的就是redis里的一个key，如果这个机器宕机了，
    // 对这个key不断的刷新其生存周期的后台定时调度的任务就没了，redis里的key，自动就会在
    // 最多leasetime秒内就过期删除
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {



        // "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
        // "return nil;" +
        // "end; " +

        // "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
        // "if (counter > 0) then " +
        // "redis.call('pexpire', KEYS[1], ARGV[2]); " +
        // "return 0; " +
        // "else " +
        // "redis.call('del', KEYS[1]); " +

        // "redis.call('publish', KEYS[2], ARGV[1]); " +
        // "return 1; " +
        // "end; " +

        // "return nil;"

        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // lua脚本的参数解释
                // KEYS[1]: getName()，即我们设置的锁名字
                // KEYS[2]: 发布订阅的key，即getChannelName()，即redisson_lock__channel_我们设置的锁名字
                // ARGV[1]: LockPubSub.UNLOCK_MESSAGE
                // ARGV[2]: internalLockLeaseTime
                // ARGV[3]: getLockName(threadId)

                // 如果KEYS[1]存在，且ARGV[3]的值为0，即表示锁已经被释放，则直接返回null，表示锁已经释放成功
                "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                        // 如果成立返回null，标示没加过锁
                        "return nil;" +
                        "end; " +

                        // 否则将锁的值加上-1，即可重入的扣减1
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        // 如果减去1后还大于0，则刷新锁的过期时间，然后返回0
                        "if (counter > 0) then " +
                        "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                        "return 0; " +
                        "else " +
                        // 执行到这里说明counter == 0，执行删除锁指令，即del
                        "redis.call('del', KEYS[1]); " +
                        // 向key为getChannelName() （redisson_lock__channel_我们设置的锁名字）中发布一个消息，然后订阅这个key的客户端可以收到redis推给他的消息
                        // 这个是redis的发布订阅模型即pub/sub
                        "redis.call('publish', KEYS[2], ARGV[1]); " +
                        // 返回1，标示删除成功
                        "return 1; " +
                        "end; " +

                        // 兜底的return策略，不会执行到这里的
                        "return nil;",
                Arrays.asList(getName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId));
    }

    // 对指定threadId进行异步释放锁
    // 其他的unlock都是调用这个
    @Override
    public RFuture<Void> unlockAsync(long threadId) {
        RPromise<Void> result = new RedissonPromise<Void>();
        // 执行释放锁逻辑
        RFuture<Boolean> future = unlockInnerAsync(threadId);

        future.onComplete((opStatus, e) -> {
            // 从看门狗维护的续约集合中删除threadId的续约，如果是可重入的会依次释放掉，不然你一次释放掉不合符逻辑操作啊
            cancelExpirationRenewal(threadId);

            if (e != null) {
                result.tryFailure(e);
                return;
            }

            if (opStatus == null) {
                IllegalMonitorStateException cause = new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                        + id + " thread-id: " + threadId);
                result.tryFailure(cause);
                return;
            }

            result.trySuccess(null);
        });

        return result;
    }

    @Override
    public RFuture<Void> lockAsync() {
        return lockAsync(-1, null);
    }

    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return lockAsync(leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Void> lockAsync(long currentThreadId) {
        return lockAsync(-1, null, currentThreadId);
    }
    
    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit, long currentThreadId) {
        RPromise<Void> result = new RedissonPromise<Void>();
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            if (e != null) {
                result.tryFailure(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                if (!result.trySuccess(null)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            RFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            subscribeFuture.onComplete((res, ex) -> {
                if (ex != null) {
                    result.tryFailure(ex);
                    return;
                }

                lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
            });
        });

        return result;
    }

    private void lockAsync(long leaseTime, TimeUnit unit,
            RFuture<RedissonLockEntry> subscribeFuture, RPromise<Void> result, long currentThreadId) {
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            if (e != null) {
                unsubscribe(subscribeFuture, currentThreadId);
                result.tryFailure(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                unsubscribe(subscribeFuture, currentThreadId);
                if (!result.trySuccess(null)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            RedissonLockEntry entry = subscribeFuture.getNow();
            if (entry.getLatch().tryAcquire()) {
                lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
            } else {
                // waiting for message
                AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
                Runnable listener = () -> {
                    if (futureRef.get() != null) {
                        futureRef.get().cancel();
                    }
                    lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                };

                entry.addListener(listener);

                if (ttl >= 0) {
                    Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                        @Override
                        public void run(Timeout timeout) throws Exception {
                            if (entry.removeListener(listener)) {
                                lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                            }
                        }
                    }, ttl, TimeUnit.MILLISECONDS);
                    futureRef.set(scheduledFuture);
                }
            }
        });
    }

    // 异步获取一次锁
    @Override
    public RFuture<Boolean> tryLockAsync() {
        return tryLockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long threadId) {
        // 这里直接返回加锁结果，不像lock，执行完获取锁后，如果失败还要等待的
        return tryAcquireOnceAsync(-1, null, threadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        return tryLockAsync(waitTime, -1, unit);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return tryLockAsync(waitTime, leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit,
            long currentThreadId) {
        RPromise<Boolean> result = new RedissonPromise<Boolean>();

        AtomicLong time = new AtomicLong(unit.toMillis(waitTime));
        long currentTime = System.currentTimeMillis();
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            if (e != null) {
                result.tryFailure(e);
                return;
            }

            // lock acquired
            if (ttl == null) {
                if (!result.trySuccess(true)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            long el = System.currentTimeMillis() - currentTime;
            time.addAndGet(-el);
            
            if (time.get() <= 0) {
                trySuccessFalse(currentThreadId, result);
                return;
            }
            
            long current = System.currentTimeMillis();
            AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
            RFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            subscribeFuture.onComplete((r, ex) -> {
                if (ex != null) {
                    result.tryFailure(ex);
                    return;
                }

                if (futureRef.get() != null) {
                    futureRef.get().cancel();
                }

                long elapsed = System.currentTimeMillis() - current;
                time.addAndGet(-elapsed);
                
                tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
            });
            if (!subscribeFuture.isDone()) {
                Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) throws Exception {
                        if (!subscribeFuture.isDone()) {
                            subscribeFuture.cancel(false);
                            trySuccessFalse(currentThreadId, result);
                        }
                    }
                }, time.get(), TimeUnit.MILLISECONDS);
                futureRef.set(scheduledFuture);
            }
        });


        return result;
    }

    private void trySuccessFalse(long currentThreadId, RPromise<Boolean> result) {
        acquireFailedAsync(currentThreadId).onComplete((res, e) -> {
            if (e == null) {
                result.trySuccess(false);
            } else {
                result.tryFailure(e);
            }
        });
    }

    private void tryLockAsync(AtomicLong time, long leaseTime, TimeUnit unit,
            RFuture<RedissonLockEntry> subscribeFuture, RPromise<Boolean> result, long currentThreadId) {
        if (result.isDone()) {
            unsubscribe(subscribeFuture, currentThreadId);
            return;
        }
        
        if (time.get() <= 0) {
            unsubscribe(subscribeFuture, currentThreadId);
            trySuccessFalse(currentThreadId, result);
            return;
        }
        
        long curr = System.currentTimeMillis();
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
                if (e != null) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    result.tryFailure(e);
                    return;
                }

                // lock acquired
                if (ttl == null) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    if (!result.trySuccess(true)) {
                        unlockAsync(currentThreadId);
                    }
                    return;
                }
                
                long el = System.currentTimeMillis() - curr;
                time.addAndGet(-el);
                
                if (time.get() <= 0) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    trySuccessFalse(currentThreadId, result);
                    return;
                }

                // waiting for message
                long current = System.currentTimeMillis();
                RedissonLockEntry entry = subscribeFuture.getNow();
                if (entry.getLatch().tryAcquire()) {
                    tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                } else {
                    AtomicBoolean executed = new AtomicBoolean();
                    AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
                    Runnable listener = () -> {
                        executed.set(true);
                        if (futureRef.get() != null) {
                            futureRef.get().cancel();
                        }

                        long elapsed = System.currentTimeMillis() - current;
                        time.addAndGet(-elapsed);
                        
                        tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                    };
                    entry.addListener(listener);

                    long t = time.get();
                    if (ttl >= 0 && ttl < time.get()) {
                        t = ttl;
                    }
                    if (!executed.get()) {
                        Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                            @Override
                            public void run(Timeout timeout) throws Exception {
                                if (entry.removeListener(listener)) {
                                    long elapsed = System.currentTimeMillis() - current;
                                    time.addAndGet(-elapsed);
                                    
                                    tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                                }
                            }
                        }, t, TimeUnit.MILLISECONDS);
                        futureRef.set(scheduledFuture);
                    }
                }
        });
    }


}
