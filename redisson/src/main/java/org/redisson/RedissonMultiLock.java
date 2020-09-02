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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.RedisResponseTimeoutException;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;
import org.redisson.misc.TransferListener;

/**
 * Groups multiple independent locks and manages them as one lock.
 *
 * @author Nikita Koksharov
 *
 */
//MultiLock，redisson分布式锁这块是支持MultiLock这个机制的，可以将多个锁合并为一个大锁，对一个大锁进行统一的申请加锁以及释放锁
//一次性锁定多个资源，再去处理一些事情，然后事后一次性释放所有的资源对应的锁
//在项目里使用的时候，很多时候一次性要锁定多个资源，比如说锁掉一个库存，锁掉一个订单，锁掉一个积分，一次性锁掉多个资源，多个资源都不让别人随意修改，然后你再一次性更新多个资源，释放多个锁
//MultiLock的源码，我们初步看一下，其实也不过是没什么特别的，就是包裹了多个RedissonLock，底层就是尝试依次对每一个锁都要成功加锁，如果所有的锁都成功加锁了之后，那么就认为MultiLock就成功加锁了
//释放锁，依次去释放每一把锁就可以
public class RedissonMultiLock implements RLock {

    class LockState {
        
        private final long newLeaseTime;
        private final long lockWaitTime;
        private final List<RLock> acquiredLocks;
        private final long waitTime;
        private final long threadId;
        private final long leaseTime;
        private final TimeUnit unit;

        private long remainTime;
        private long time = System.currentTimeMillis();
        private int failedLocksLimit;
        
        LockState(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
            this.waitTime = waitTime;
            this.leaseTime = leaseTime;
            this.unit = unit;
            this.threadId = threadId;
            
            if (leaseTime != -1) {
                if (waitTime == -1) {
                    newLeaseTime = unit.toMillis(leaseTime);
                } else {
                    newLeaseTime = unit.toMillis(waitTime)*2;
                }
            } else {
                newLeaseTime = -1;
            }

            remainTime = -1;
            if (waitTime != -1) {
                remainTime = unit.toMillis(waitTime);
            }
            lockWaitTime = calcLockWaitTime(remainTime);
            
            failedLocksLimit = failedLocksLimit();
            acquiredLocks = new ArrayList<>(locks.size());
        }
        
        void tryAcquireLockAsync(ListIterator<RLock> iterator, RPromise<Boolean> result) {
            if (!iterator.hasNext()) {
                checkLeaseTimeAsync(result);
                return;
            }

            RLock lock = iterator.next();
            RPromise<Boolean> lockAcquiredFuture = new RedissonPromise<Boolean>();
            if (waitTime == -1 && leaseTime == -1) {
                lock.tryLockAsync(threadId)
                    .onComplete(new TransferListener<Boolean>(lockAcquiredFuture));
            } else {
                long awaitTime = Math.min(lockWaitTime, remainTime);
                lock.tryLockAsync(awaitTime, newLeaseTime, TimeUnit.MILLISECONDS, threadId)
                    .onComplete(new TransferListener<Boolean>(lockAcquiredFuture));
            }
            
            lockAcquiredFuture.onComplete((res, e) -> {
                boolean lockAcquired = false;
                if (res != null) {
                    lockAcquired = res;
                }

                if (e instanceof RedisResponseTimeoutException) {
                    unlockInnerAsync(Arrays.asList(lock), threadId);
                }
                
                if (lockAcquired) {
                    acquiredLocks.add(lock);
                } else {
                    if (locks.size() - acquiredLocks.size() == failedLocksLimit()) {
                        checkLeaseTimeAsync(result);
                        return;
                    }

                    if (failedLocksLimit == 0) {
                        unlockInnerAsync(acquiredLocks, threadId).onComplete((r, ex) -> {
                            if (ex != null) {
                                result.tryFailure(ex);
                                return;
                            }
                            
                            if (waitTime == -1) {
                                result.trySuccess(false);
                                return;
                            }
                            
                            failedLocksLimit = failedLocksLimit();
                            acquiredLocks.clear();
                            // reset iterator
                            while (iterator.hasPrevious()) {
                                iterator.previous();
                            }
                            
                            checkRemainTimeAsync(iterator, result);
                        });
                        return;
                    } else {
                        failedLocksLimit--;
                    }
                }
                
                checkRemainTimeAsync(iterator, result);
            });
        }
        
        private void checkLeaseTimeAsync(RPromise<Boolean> result) {
            if (leaseTime != -1) {
                AtomicInteger counter = new AtomicInteger(acquiredLocks.size());
                for (RLock rLock : acquiredLocks) {
                    RFuture<Boolean> future = ((RedissonLock) rLock).expireAsync(unit.toMillis(leaseTime), TimeUnit.MILLISECONDS);
                    future.onComplete((res, e) -> {
                        if (e != null) {
                            result.tryFailure(e);
                            return;
                        }
                        
                        if (counter.decrementAndGet() == 0) {
                            result.trySuccess(true);
                        }
                    });
                }
                return;
            }
            
            result.trySuccess(true);
        }
        
        private void checkRemainTimeAsync(ListIterator<RLock> iterator, RPromise<Boolean> result) {
            if (remainTime != -1) {
                remainTime += -(System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                if (remainTime <= 0) {
                    unlockInnerAsync(acquiredLocks, threadId).onComplete((res, e) -> {
                        if (e != null) {
                            result.tryFailure(e);
                            return;
                        }
                        
                        result.trySuccess(false);
                    });
                    return;
                }
            }
            
            tryAcquireLockAsync(iterator, result);
        }
        
    }

    // 持有着多个RLock引用
    final List<RLock> locks = new ArrayList<>();
    
    /**
     * Creates instance with multiple {@link RLock} objects.
     * Each RLock object could be created by own Redisson instance.
     *
     * @param locks - array of locks
     */
    public RedissonMultiLock(RLock... locks) {
        if (locks.length == 0) {
            throw new IllegalArgumentException("Lock objects are not defined");
        }
        this.locks.addAll(Arrays.asList(locks));
    }
    
    @Override
    public void lock() {
        try {
            lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void lock(long leaseTime, TimeUnit unit) {
        try {
            lockInterruptibly(leaseTime, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
        return lockAsync(leaseTime, unit, Thread.currentThread().getId());
    }
    
    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit, long threadId) {
        long baseWaitTime = locks.size() * 1500;
        long waitTime = -1;
        if (leaseTime == -1) {
            waitTime = baseWaitTime;
        } else {
            leaseTime = unit.toMillis(leaseTime);
            waitTime = leaseTime;
            if (waitTime <= 2000) {
                waitTime = 2000;
            } else if (waitTime <= baseWaitTime) {
                waitTime = ThreadLocalRandom.current().nextLong(waitTime/2, waitTime);
            } else {
                waitTime = ThreadLocalRandom.current().nextLong(baseWaitTime, waitTime);
            }
        }
        
        RPromise<Void> result = new RedissonPromise<Void>();
        tryLockAsync(threadId, leaseTime, TimeUnit.MILLISECONDS, waitTime, result);
        return result;
    }
    
    protected void tryLockAsync(long threadId, long leaseTime, TimeUnit unit, long waitTime, RPromise<Void> result) {
        tryLockAsync(waitTime, leaseTime, unit, threadId).onComplete((res, e) -> {
            if (e != null) {
                result.tryFailure(e);
                return;
            }
            
            if (res) {
                result.trySuccess(null);
            } else {
                tryLockAsync(threadId, leaseTime, unit, waitTime, result);
            }
        });
    }


    @Override
    public void lockInterruptibly() throws InterruptedException {
        lockInterruptibly(-1, null);
    }

    @Override
    public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
        long baseWaitTime = locks.size() * 1500;
        long waitTime = -1;
        if (leaseTime == -1) {
            // 如果leaseTime=-1，即启动看门狗续约。这里设置waitTime为locks.size() * 1500
            waitTime = baseWaitTime;
        } else {
            // 走到这，说明leaseTime不是-1.加锁后不启动看门狗续约
            // 转换续约时间，并计算等待时间
            leaseTime = unit.toMillis(leaseTime);
            waitTime = leaseTime;
            if (waitTime <= 2000) {
                waitTime = 2000;
            } else if (waitTime <= baseWaitTime) {
                waitTime = ThreadLocalRandom.current().nextLong(waitTime/2, waitTime);
            } else {
                waitTime = ThreadLocalRandom.current().nextLong(baseWaitTime, waitTime);
            }
        }
        
        while (true) {
            if (tryLock(waitTime, leaseTime, TimeUnit.MILLISECONDS)) {
                return;
            }
        }
    }

    @Override
    public boolean tryLock() {
        try {
            return tryLock(-1, -1, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    protected void unlockInner(Collection<RLock> locks) {
        List<RFuture<Void>> futures = new ArrayList<>(locks.size());
        for (RLock lock : locks) {
            futures.add(lock.unlockAsync());
        }

        for (RFuture<Void> unlockFuture : futures) {
            unlockFuture.awaitUninterruptibly();
        }
    }
    
    protected RFuture<Void> unlockInnerAsync(Collection<RLock> locks, long threadId) {
        if (locks.isEmpty()) {
            return RedissonPromise.newSucceededFuture(null);
        }
        
        RPromise<Void> result = new RedissonPromise<Void>();
        AtomicInteger counter = new AtomicInteger(locks.size());
        for (RLock lock : locks) {
            lock.unlockAsync(threadId).onComplete((res, e) -> {
                if (e != null) {
                    result.tryFailure(e);
                    return;
                }
                
                if (counter.decrementAndGet() == 0) {
                    result.trySuccess(null);
                }
            });
        }
        return result;
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        return tryLock(waitTime, -1, unit);
    }
    
    protected int failedLocksLimit() {
        return 0;
    }
    
    @Override
    public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
//        try {
//            return tryLockAsync(waitTime, leaseTime, unit).get();
//        } catch (ExecutionException e) {
//            throw new IllegalStateException(e);
//        }
        long newLeaseTime = -1;
        if (leaseTime != -1) {
            if (waitTime == -1) {
                newLeaseTime = unit.toMillis(leaseTime);
            } else {
                // 续约时间扩大2倍
                newLeaseTime = unit.toMillis(waitTime)*2;
            }
        }
        
        long time = System.currentTimeMillis();
        long remainTime = -1;
        if (waitTime != -1) {
            remainTime = unit.toMillis(waitTime);
        }
        // 计算一个加锁等待时间，其实就是remainTime
        long lockWaitTime = calcLockWaitTime(remainTime);

        // failedLocksLimit()返回的是0，即不允许任意一把锁加锁失败
        int failedLocksLimit = failedLocksLimit();
        List<RLock> acquiredLocks = new ArrayList<>(locks.size());
        // 遍历locks
        for (ListIterator<RLock> iterator = locks.listIterator(); iterator.hasNext();) {
            RLock lock = iterator.next();
            boolean lockAcquired;
            try {
                if (waitTime == -1 && leaseTime == -1) {
                    lockAcquired = lock.tryLock();
                } else {
                    // 拿到一个最小的等待时间
                    long awaitTime = Math.min(lockWaitTime, remainTime);
                    // 支持超时的加锁
                    lockAcquired = lock.tryLock(awaitTime, newLeaseTime, TimeUnit.MILLISECONDS);
                }
            } catch (RedisResponseTimeoutException e) {
                unlockInner(Arrays.asList(lock));
                lockAcquired = false;
            } catch (Exception e) {
                lockAcquired = false;
            }
            
            if (lockAcquired) {
                // 加锁成功，将锁放入成功加锁集合中
                acquiredLocks.add(lock);
            } else {
                // 走到这说明当前这把锁加锁失败了。

                // 条件成立：说明当前加锁失败的数量和允许失败的数量一样。退出循环
                if (locks.size() - acquiredLocks.size() == failedLocksLimit()) {
                    break;
                }

                // 条件成立：不允许任意一把锁加锁失败
                if (failedLocksLimit == 0) {
                    // 因为当前这把锁加锁失败了，所以将已经成功加的锁都释放掉
                    unlockInner(acquiredLocks);
                    if (waitTime == -1) {
                        // 如果不等待，直接返回false，加锁失败
                        return false;
                    }
                    failedLocksLimit = failedLocksLimit();
                    // 清空成功锁集合
                    acquiredLocks.clear();
                    // reset iterator
                    // 重置迭代器
                    while (iterator.hasPrevious()) {
                        iterator.previous();
                    }
                } else {
                    failedLocksLimit--;
                }
            }
            
            if (remainTime != -1) {
                remainTime -= System.currentTimeMillis() - time;
                time = System.currentTimeMillis();
                if (remainTime <= 0) {
                    unlockInner(acquiredLocks);
                    return false;
                }
            }
        }

        if (leaseTime != -1) {
            List<RFuture<Boolean>> futures = new ArrayList<>(acquiredLocks.size());
            for (RLock rLock : acquiredLocks) {
                RFuture<Boolean> future = ((RedissonLock) rLock).expireAsync(unit.toMillis(leaseTime), TimeUnit.MILLISECONDS);
                futures.add(future);
            }
            
            for (RFuture<Boolean> rFuture : futures) {
                rFuture.syncUninterruptibly();
            }
        }
        
        return true;
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        RPromise<Boolean> result = new RedissonPromise<Boolean>();
        LockState state = new LockState(waitTime, leaseTime, unit, threadId);
        state.tryAcquireLockAsync(locks.listIterator(), result);
        return result;
    }
    
    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
        return tryLockAsync(waitTime, leaseTime, unit, Thread.currentThread().getId());
    }

    
    protected long calcLockWaitTime(long remainTime) {
        return remainTime;
    }

    @Override
    public RFuture<Void> unlockAsync(long threadId) {
        return unlockInnerAsync(locks, threadId);
    }
    
    @Override
    public void unlock() {
        List<RFuture<Void>> futures = new ArrayList<>(locks.size());

        for (RLock lock : locks) {
            futures.add(lock.unlockAsync());
        }

        for (RFuture<Void> future : futures) {
            future.syncUninterruptibly();
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Void> unlockAsync() {
        return unlockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Boolean> tryLockAsync() {
        return tryLockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Void> lockAsync() {
        return lockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Void> lockAsync(long threadId) {
        return lockAsync(-1, null, threadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long threadId) {
        return tryLockAsync(-1, -1, null, threadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        return tryLockAsync(waitTime, -1, unit);
    }

    @Override
    public RFuture<Integer> getHoldCountAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forceUnlock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocked() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public RFuture<Boolean> isLockedAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHeldByThread(long threadId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHoldCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Long> remainTimeToLiveAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long remainTimeToLive() {
        throw new UnsupportedOperationException();
    }

}
