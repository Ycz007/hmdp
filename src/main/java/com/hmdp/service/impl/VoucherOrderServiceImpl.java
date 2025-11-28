package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {


    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    IVoucherOrderService proxy;

    //异步线程池，单线程处理
    private static final ExecutorService SECKILL_ORDER_EXECUTOR= Executors.newSingleThreadExecutor();
    @PostConstruct
    public void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

        public class VoucherOrderHandler implements Runnable {
            String queneName="stream.orders";
        @Override
        public void run() {

              while (true) {
                  try {
                      //1.从消息队列中取订单 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 stream.orders >
                      List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                              Consumer.from("g1", "c1"),
                              StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                              StreamOffset.create(queneName, ReadOffset.lastConsumed())
                      );
                      //2.判断消息获取是否成功
                      if(list==null || list.isEmpty()){
                          //如果获取失败，说明没有消息，继续下一次循环
                          continue;
                      }

                      //3.有消息，可以下单
                      //3.1解析消息
                      MapRecord<String, Object, Object> record = list.get(0);
                      Map<Object, Object> value = record.getValue();
                      VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                      //4.创建订单
                      handleVoucherOrder(voucherOrder);
                      //5.ack确认
                      stringRedisTemplate.opsForStream().acknowledge(queneName,"g1",record.getId());
                  } catch (Exception e) {
                      log.error("处理penging-list订单异常",e);
                      handlePendingList();
                  }

              }

        }

            private void handlePendingList() {
                while (true) {
                    try {
                        //1.从消息队列中取订单 XREADGROUP GROUP g1 c1 COUNT 1  2000 stream.orders 0
                        List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                                Consumer.from("g1", "c1"),
                                StreamReadOptions.empty().count(1),
                                StreamOffset.create(queneName, ReadOffset.from("0"))
                        );
                        //2.判断消息获取是否成功
                        if(list==null || list.isEmpty()){
                            //如果获取失败，说明penginglist没有异常消息，结束循环
                            break;
                        }

                        //3.有消息，可以下单
                        //3.1解析消息
                        MapRecord<String, Object, Object> record = list.get(0);
                        Map<Object, Object> value = record.getValue();
                        VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                        //4.创建订单
                        handleVoucherOrder(voucherOrder);
                        //5.ack确认
                        stringRedisTemplate.opsForStream().acknowledge(queneName,"g1",record.getId());
                    } catch (Exception e) {
                        log.error("处理订单异常",e);
                    }

                }
            }



        }



    //阻塞队列
//    private BlockingQueue<VoucherOrder> orderTask=new ArrayBlockingQueue<>(1024*1024);
//    public class VoucherOrderHandler implements Runnable {
//
//        @Override
//        public void run() {
//              while (true) {
//                  try {
//                      //从阻塞队列中取订单
//                      VoucherOrder voucherOrder = orderTask.take();
//                      handleVoucherOrder(voucherOrder);
//                  } catch (Exception e) {
//                      log.error("处理订单异常",e);
//                  }
//
//              }
//
//        }
//    }
//
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        boolean islock=lock.tryLock();
        if(!islock){
            log.error("不允许重复下单");
            return;
        }

        try{
            proxy.createVoucher(voucherOrder);
        }finally {
            lock.unlock();
        }

    }


    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取订单id
        long orderId=redisIdWorker.nextId("order");
        //获取用户
        Long userId=UserHolder.getUser().getId();
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );
        //2.判断是否为0
        int r=result.intValue();
        //2.1 不为0没有资格返回
        if(r!=0){
            return Result.fail(r==1? "库存不足":"不能重复下单");
        }

        //2.2  有资格

        proxy=(IVoucherOrderService) AopContext.currentProxy();
        //3.返回订单id
        return Result.ok(orderId);
    }


//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        Long userId=UserHolder.getUser().getId();
//      //1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(),
//                userId.toString()
//        );
//        //2.判断是否为0
//       int r=result.intValue();
//      //2.1 不为0没有资格返回
//        if(r!=0){
//            return Result.fail(r==1? "库存不足":"不能重复下单");
//        }
//
//      //2.2  有资格
//      long orderId=redisIdWorker.nextId("order");
//
//
//        VoucherOrder voucherOrder=new VoucherOrder();
//      //2.3 订单id
//        voucherOrder.setId(orderId);
//        //2.4用户id
//        voucherOrder.setUserId(userId);
//      //2.5 优惠券id
//        voucherOrder.setVoucherId(voucherId);
//      // 2.6 放入阻塞队列，异步执行
//        orderTask.add(voucherOrder);
//        proxy=(IVoucherOrderService) AopContext.currentProxy();
//       //3.返回订单id
//       return Result.ok(orderId);
//    }


//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //1.查询优惠卷
//        SeckillVoucher voucher =seckillVoucherService.getById(voucherId);
//        //2.判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀未开始");
//        }
//        //3.判断秒杀是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束");
//        }
//        //4.判断是否有库存
//        if (voucher.getStock()<1) {
//            return Result.fail("优惠卷已被抢完");
//        }
//        //5.扣减库存
//        Boolean success=seckillVoucherService
//                .update()
//                .setSql("stock=stock-1")
//                .eq("voucher_id",voucherId).gt("stock",0)
//                .update();
//
//        if (!success) {
//            return Result.fail("库存不足");
//        }
//
//        //获取锁-提交事务-释放锁
//
//        Long userId = UserHolder.getUser().getId();
//
////        SimpleRedisLock lock=new SimpleRedisLock("order:"+userId,stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//
//
//        boolean islock=lock.tryLock();
//
//        if(!islock){
//            return Result.fail("一个人只能购买一次");
//
//        }
//        try{
//            //获取代理对象（事务）
//            IVoucherOrderService proxy =(IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucher(voucherId);
//        }finally {
//            lock.unlock();
//        }
//
//    }




    @Transactional
    public  void createVoucher(VoucherOrder voucherOrder) {
        //一人一单
        Long userId = voucherOrder.getUserId();
            int count = query().eq("voucher_id", voucherOrder).eq("user_id", userId).count();
            if(count>0){
                log.error("只能购买一次");
               return ;
            }


        Boolean success=seckillVoucherService
                .update()
                .setSql("stock=stock-1")
                .eq("voucher_id",voucherOrder.getVoucherId()).gt("stock",0)
                .update();

       if (!success) {
           log.error("库存不足");
           return ;
        }
        //7.返回订单id
        save(voucherOrder);
    }
}
