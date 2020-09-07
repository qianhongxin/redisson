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

import java.util.List;

import org.redisson.api.RLock;

/**
 * RedLock locking algorithm implementation for multiple locks. 
 * It manages all locks as one.
 * 
 * @see <a href="http://redis.io/topics/distlock">http://redis.io/topics/distlock</a>
 *
 * @author Nikita Koksharov
 *
 */
//这个场景是假设有一个redis cluster，有3个redis master实例
//
//然后执行如下步骤获取一把分布式锁：
//
//1）获取当前时间戳，单位是毫秒
//2）跟上面类似，轮流尝试在每个master节点上创建锁，过期时间较短，一般就几十毫秒，在每个节点上创建锁的过程中，需要加一个超时时间，一般来说比如几十毫秒如果没有获取到锁就超时了，标识为获取锁失败
//3）尝试在大多数节点上建立一个锁，比如3个节点就要求是2个节点（n / 2 +1）
//4）客户端计算建立好锁的时间，如果建立锁的时间小于超时时间，就算建立成功了
//5）要是锁建立失败了，那么就依次删除已经创建的锁
//6）只要别人创建了一把分布式锁，你就得不断轮询去尝试获取锁
//
//他这里最最核心的一个点，普通的redis分布式锁，其实是在redis集群中根据hash算法选择一台redis实例创建一个锁就可以了
//RedLock算法思想，不能只在一个redis实例上创建锁，应该是在多个redis实例上创建锁，n / 2 + 1，必须在大多数redis节点上都成功创建锁，才能算这个整体的RedLock加锁成功，避免说仅仅在一个redis实例上加锁

// RedLock也是基于RedissonMultiLock实现的。对多个实例加锁，加的锁都是一样的


// 不靠谱的：
// RedLock算法，底层是对应着多个小lock，每个小lock应该是在一个redis实例上去的，他每次都要在大多数的redis master实例上加锁成功，3个master实例，2个master实例上加锁成功，才算是一把锁加成功了
//
//有一个客户端，在3个master实例，假设成功在里面加了3个master实例的锁， 不幸的是其中一台master突然宕机，还没同步到slave实例上去，此时他的slave切换成了新的master。
//
//另外一个客户端尝试加锁，此时只能在那个新切换过来的那个master实例上加锁，另外两个master是无法成功加锁的，这样就能保证他加锁是不会成功的
//
//这个算法就是所谓的RedLock算法
//
//大家可以自己去尝试分析一下，枚举一下各种情况，这个算法还是有漏洞的
//
//5个master实例，客户端A尝试加锁，仅仅成功的在3个master实例加了锁，成功了；此时不幸的是此时3个master中的1个master突然宕机了，锁key还没同步到他的slave实例上去，此时salve切换为新的master
//
//5个master，其中一个是新切换过来的master，其实只有2个master是有客户端A加锁的一个痕迹的，另外3个master是没有这个锁key的
//
//然后的不幸的是，此时客户端B来加锁，他其实很有可能可以成功的在3个master上成功加锁，达到了一个大多数的数字，完成了加锁，还是会发生说多个客户端同时重复加锁，所以说也是不是完全靠谱的
public class RedissonRedLock extends RedissonMultiLock {

    /**
     * Creates instance with multiple {@link RLock} objects.
     * Each RLock object could be created by own Redisson instance.
     *
     * @param locks - array of locks
     */
    public RedissonRedLock(RLock... locks) {
        super(locks);
    }

    @Override
    // 设置允许失败的数量
    protected int failedLocksLimit() {
        return locks.size() - minLocksAmount(locks);
    }
    
    protected int minLocksAmount(final List<RLock> locks) {
        return locks.size()/2 + 1;
    }

    // 设置等待时间
    @Override
    protected long calcLockWaitTime(long remainTime) {
        return Math.max(remainTime / locks.size(), 1);
    }
    
    @Override
    public void unlock() {
        unlockInner(locks);
    }

}
