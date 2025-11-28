package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService  followService;

    @Override
    public Result queryHotBlog(Integer current) {

            // 根据用户查询
            Page<Blog> page = query()
                    .orderByDesc("liked")
                    .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
            // 获取当前页数据
            List<Blog> records = page.getRecords();
            // 查询用户
            records.forEach(blog ->{
                Long userId = blog.getUserId();
                User user = userService.getById(userId);
                blog.setName(user.getNickName());
                blog.setIcon(user.getIcon());
                isBlogLike(blog);
            });
            return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        //1.查询blog
        Blog blog = getById(id);
        if(blog == null){
            return Result.fail("博客不存在");
        }

        //2.查询和blog相关用户
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
        isBlogLike(blog);

        return Result.ok(blog);
    }

    public void isBlogLike(Blog blog){
        UserDTO user = UserHolder.getUser();
        if(user==null){
            return;
        }
        Long userId = user.getId();
        String key="blog:liked:"+blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score!=null);
    }




    @Override
    public Result likeBlog(Long id) {
        //1.获取用户
        Long userId = UserHolder.getUser().getId();
        //2.判断是否点赞
        String key="blog:liked:"+id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());

        if(score==null) {
            //3.未点赞
            //3.1数据库+1
            boolean isSuccess = update().setSql("liked=liked+1").eq("id", id).update();
            //3.2存入redis
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        }
        else {
            //4.已经点赞
            //4.1数据库-1
            boolean isSuccess = update().setSql("liked=liked-1").eq("id", id).update();
            //4.2移除redis
            if(isSuccess){
                stringRedisTemplate.opsForZSet().remove(key,userId.toString());
            }
        }

        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY + id;
        // 1.查询top5的点赞用户 zrange key 0 4
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        // 2.解析出其中的用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids);
        // 3.根据用户id查询用户 WHERE id IN ( 5 , 1 ) ORDER BY FIELD(id, 5, 1)
        List<UserDTO> userDTOS = userService.query()
                .in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        // 4.返回
        return Result.ok(userDTOS);
    }

    //新增文章
    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = save(blog);
        if (!isSuccess) {
            return Result.fail("新增笔记失败");
        }

       //查询关注“我”的人
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();

        //发送笔记
        for(Follow follow:follows){
            Long userId = follow.getUserId();
            String key="feed:"+userId;
            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
        }

        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogByFollow(Long max, Integer offset) {
        //1.获取用户id
        Long userId = UserHolder.getUser().getId();
        //2.获取收件箱 ZREVERANGEBYSCORE key Max Min LIMIT offset count
        String key="feed:"+userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate
                .opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        //获取blogId和时间

        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }

        List<Long> ids=new ArrayList<>(typedTuples.size());
        long minTime=0;
        int of=1;
        //3.查询
        for(ZSetOperations.TypedTuple<String> tuple:typedTuples){
            //3.1获取id
            String idStr = tuple.getValue();
            ids.add(Long.valueOf(idStr));
            //3.2时间戳
            long time = tuple.getScore().longValue();

            if(time==minTime){
                of++;
            }else {
                minTime = time;
                of=1;
            }
        }

        of = minTime == max ? of : of + offset;
        //4.根据id查询blog
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogList = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();

        //5.查出blog的user和点赞
        for(Blog blog:blogList){
            //5.1查询用户
            Long userid = blog.getUserId();
            User user = userService.getById(userid);
            blog.setName(user.getNickName());
            blog.setIcon(user.getIcon());
            //5.2判断点赞
            isBlogLike(blog);
        }

        //6.返回结果
        ScrollResult r=new ScrollResult();
        r.setMinTime(minTime);
        r.setOffset(of);
        r.setList(blogList);

        return Result.ok(r);
    }
}
