/**************Scheduler::****************************************************************
 * Copyright 2018 The Apollo Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Vesched_infoon 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/

#include "cyber/task/task_manager.h"

#include "cyber/common/global_data.h"
#include "cyber/croutine/croutine.h"
#include "cyber/croutine/routine_factory.h"
#include "cyber/scheduler/scheduler_factory.h"

namespace apollo {
namespace cyber {

using apollo::cyber::common::GlobalData;
static const char* const task_prefix = "/internal/task";

TaskManager::TaskManager()
    : task_queue_size_(1000),
      task_queue_(new base::BoundedQueue<std::function<void()>>()) {
  // 初始化任务队列对象
  if (!task_queue_->Init(task_queue_size_, new base::BlockWaitStrategy())) {
    AERROR << "Task queue init failed";
    throw std::runtime_error("Task queue init failed");
  }
  // 轮询从任务队列中取出task并执行
  auto func = [this]() {
    while (!stop_) {
      std::function<void()> task;
      // 从task_queue_中取一个放入task
      // 获取成功时，执行task函数
      // 获取失败时，即队列为空，将当前协程挂起
      if (!task_queue_->Dequeue(&task)) {
        auto routine = croutine::CRoutine::GetCurrentRoutine();
        // 取不到任务时将协程state_置为DATA_WAIT并切换协程上下文
        routine->HangUp();
        continue;
      }
      task();
    }
  };

  /*
  * 从scheduler获取线程数，也即processor数
  * 定义RoutineFactory对象，通过该对象可将绑定的函数和datavisitor联系起来
  * 为每个线程创建一个任务task
  * tasks_中保存的是task_name对应的哈希键值
  * 调用scheduler创建task，将factory的函数绑定到创建的协程上并唤醒一个优先级高的协程
  * 绑定的函数是循环从taskqueue中取任务并执行
  * ////这里createTask时datavisitor为空会有什么影响？？？
  * ////协程有优先级而task没有优先级，那协程的优先级还有意义吗？？？
  */
  num_threads_ = scheduler::Instance()->TaskPoolSize();
  auto factory = croutine::CreateRoutineFactory(std::move(func));
  tasks_.reserve(num_threads_);
  for (uint32_t i = 0; i < num_threads_; i++) {
    auto task_name = task_prefix + std::to_string(i);
    tasks_.push_back(common::GlobalData::RegisterTaskName(task_name));
    // 此处创建协程
    if (!scheduler::Instance()->CreateTask(factory, task_name)) {
      AERROR << "CreateTask failed:" << task_name;
    }
  }
}

TaskManager::~TaskManager() { Shutdown(); }

void TaskManager::Shutdown() {
  if (stop_.exchange(true)) {
    return;
  }
  for (uint32_t i = 0; i < num_threads_; i++) {
    scheduler::Instance()->RemoveTask(task_prefix + std::to_string(i));
  }
}

}  // namespace cyber
}  // namespace apollo
