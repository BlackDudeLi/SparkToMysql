package com.shifang.live.realtime.bean

/**
 * 直播样例类
 * @param user_id
 * @param content_id
 * @param live_id
 * @param live_number
 * @param join_time
 */
case class LiveInfo(
                     user_id: Int,
                     content_id: Int,
                     live_id: Int,
                     live_number: Int,
                     join_time: String
                   )
