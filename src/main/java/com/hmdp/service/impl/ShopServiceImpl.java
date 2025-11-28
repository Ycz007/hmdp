package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.sun.org.apache.xpath.internal.operations.Bool;
import io.micrometer.core.instrument.util.JsonUtils;
import jdk.nashorn.internal.ir.CallNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result QueryById(Long id) {
      //缓存穿透
      //  Shop shop=QueryWithPassThrough(id);

      //互斥锁解决缓存击穿
        Shop shop=QueryByMutex(id);
        if(shop==null){
            return Result.fail("店铺不存在");
        }
     return Result.ok(shop);
    }

    public Shop QueryByMutex(Long id){
        //查询缓存
        String key=RedisConstants.CACHE_SHOP_KEY+id;
        String json = stringRedisTemplate.opsForValue().get(key);

        //""不返回，有值才返回
        if(StrUtil.isNotBlank(json)){
            //缓存存在直接返回
            Shop shop=JSONUtil.toBean(json,Shop.class);
            return shop;
        }

        //""
        //判断是否是空值
        if(json!=null){
            return null;
        }

        //4.缓存重建
        Shop shop=null;
        String lockKey="lock:shop:"+id;
        try{
        Boolean isLock=tryLock(lockKey);
        if(!isLock){
            Thread.sleep(50);
            return QueryByMutex(id);
        }
        //不存在查询数据库
        shop = getById(id);
        if(shop==null){
            stringRedisTemplate.opsForValue().set(key,"", RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }

        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);}
        catch (Exception e){
            throw new RuntimeException();
        }
        finally{
            unLock(lockKey);
        }
        return shop;

    }


    public Shop QueryWithPassThrough(Long id){
        //查询缓存
        String key=RedisConstants.CACHE_SHOP_KEY+id;
        String json = stringRedisTemplate.opsForValue().get(key);

        if(StrUtil.isNotBlank(json)){
            //缓存存在直接返回
            Shop shop=JSONUtil.toBean(json,Shop.class);
            return shop;
        }

        if(json!=null){
            return null;
        }

        //不存在查询数据库
        Shop shop = getById(id);
        if(shop==null){
            stringRedisTemplate.opsForValue().set(key,"", RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }

        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }


    //更新店铺
    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long id=shop.getId();
        if(id==null){
            return Result.fail("店铺id不能为空");
        }

        //操作数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY+id);

        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否需要根据坐标查询
        if (x==null || y==null) {
            //不需要坐标查询，按数据库查询
            Page<Shop> page=query()
                            .eq("type_id",typeId)
                            .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            //返回数据
            return Result.ok(page);
        }
        //2.计算分页参数
        int from=(current-1)*SystemConstants.DEFAULT_PAGE_SIZE;
        int end=current*SystemConstants.DEFAULT_PAGE_SIZE;

        //3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key="shop:geo:"+typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results=stringRedisTemplate.opsForGeo().search(
                        key,
                        GeoReference.fromCoordinate(x,y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        //4.解析出id
        if(results==null){
            return Result.ok(Collections.emptyList());
        }

        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size()<=from){
            return Result.ok(Collections.emptyList());
        }
        //获取id
        List<Long> ids = new ArrayList<>(list.size());
        //获取距离
        Map<String,Distance> distanceMap=new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            //获取id
            String shopIdStr =result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));

            Distance distance=result.getDistance();
            distanceMap.put(shopIdStr,distance);
        });
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD (id," + idStr + ") ").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }

        //6.返回
        return Result.ok(shops);
    }

    private Boolean tryLock(String key){
        Boolean flag=stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private Boolean  unLock(String key){
        Boolean flag=stringRedisTemplate.delete(key);
        return BooleanUtil.isTrue(flag);
    }

}
