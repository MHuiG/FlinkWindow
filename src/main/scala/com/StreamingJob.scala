/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingJob {
  def main(args: Array[String]) {
    //设置环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //指定数据源，读取socket
    val socketStream = env.socketTextStream("localhost", 9000, '\n')
    //对数据集指定转换操作逻辑
    val count = socketStream
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(1)
//      .timeWindow(Time.minutes(1)) //time Window 每分钟统计一次数量和
//      .timeWindow(Time.minutes(1), Time.seconds(30)) //隔 30s 统计过去1m和
//      .countWindow(3)//统计每 3 个元素的数量之和
//      .countWindow(4, 3) //每隔 3 个元素统计过去 4 个元素的数量之和
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))//表示如果 5s 内没出现数据则认为超出会话时长，然后计算这个窗口的和
      .sum(1)
    //将计算结果打印到控制台
    count.print()
    //指定任务名称并触发流式任务
    env.execute("Socket Stream")
  }
}
