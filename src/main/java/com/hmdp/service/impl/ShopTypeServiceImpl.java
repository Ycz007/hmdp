package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public List<ShopType> queryTypeList() {
        //查询缓存内是否有数据，有就返回
        String key="shop_type";
        String jsonResult= stringRedisTemplate.opsForValue().get(key);

        if(jsonResult!=null){
           List<ShopType> typeList= JSONUtil.toList(jsonResult, ShopType.class);
            return typeList;
        }

        //没有就去数据库查询，并添加缓存
        List<ShopType> typeList=list();
        String list= JSONUtil.toJsonStr(typeList);
        stringRedisTemplate.opsForValue().set(key,list);

        return typeList;
    }
}
