/******************************************************************************
 * Copyright 2018 The Apollo Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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

#ifndef CYBER_BASE_THREAD_POOL_H_
#define CYBER_BASE_THREAD_POOL_H_

#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <queue>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "cyber/base/bounded_queue.h"

namespace apollo {
namespace cyber {
namespace base {

class ThreadPool {
 public:
  explicit ThreadPool(std::size_t thread_num, std::size_t max_task_num = 1000);

  template <typename F, typename... Args>
  auto Enqueue(F&& f, Args&&... args)
      -> std::future<typename std::result_of<F(Args...)>::type>;

  ~ThreadPool();

 private:
  std::vector<std::thread> workers_;
  BoundedQueue<std::function<void()>> task_queue_;
  std::atomic_bool stop_;
};

inline ThreadPool::ThreadPool(std::size_t threads, std::size_t max_task_num)
    : stop_(false) {
  if (!task_queue_.Init(max_task_num, new BlockWaitStrategy())) {
    throw std::runtime_error("Task queue init failed.");
  }
  workers_.reserve(threads);
  for (size_t i = 0; i < threads; ++i) {
    workers_.emplace_back([this] {
      while (!stop_) {
        std::function<void()> task;
        if (task_queue_.WaitDequeue(&task)) {
          task();
        }
      }
    });
  }
}

// before using the return value, you should check value.valid()
/*
* 定义enqueue方法
* std::future<typename std::result_of<F(Args...)>::type>是函数的返回值类型
* auto相当于返回值类型的占位符
* 这里函数enqueue的返回值类型为std::future对象，future本身的模板参数为函数Ｆ的返回值类型
* enqueue使用可变模板参数，参数个数任意多，且任意类型
* f是传入的函数指针，args是ｆ的参数列表
*/
template <typename F, typename... Args>
auto ThreadPool::Enqueue(F&& f, Args&&... args)
    -> std::future<typename std::result_of<F(Args...)>::type> {
  using return_type = typename std::result_of<F(Args...)>::type;
  /*
  * 定义packaged_task对象并通过shared_ptr持有该对象
  * packaged_task包含要执行的任务F，完成F后将其结果保存在shared_state中，并可以通过future对象获取该值
  * 一般将packaged_task绑定到"子"线程上执行，在"主"线程上通过future获取结果，实现异步执行和线程同步
  * 这里将task添加到任务队列等待被调度执行，而对应的future返回给enqueue的调用者等待获取结果
  * 该task的返回值类型为模板参数中的return_type
  * 通过bind绑定真正的任务“函数”F及其参数值，不需要填写模板参数中的形参表，否则应该写成如下形式：
  * std::make_shared<std::packaged_task<return_type(Args...)>>>( f(Args... args) )  ???
  */
  auto task = std::make_shared<std::packaged_task<return_type()>>(
      std::bind(std::forward<F>(f), std::forward<Args>(args)...));
  // 将packaged_task对象和future对象绑定
  std::future<return_type> res = task->get_future();  // 有效的future只能移动构造

  // don't allow enqueueing after stopping the pool
  if (stop_) {
    return std::future<return_type>();  // 默认构造函数定义空的对象
  }
  task_queue_.Enqueue([task]() { (*task)(); });
  return res;
};

// the destructor joins all threads
inline ThreadPool::~ThreadPool() {
  if (stop_.exchange(true)) {
    return;
  }
  task_queue_.BreakAllWait();
  for (std::thread& worker : workers_) {
    worker.join();
  }
}

}  // namespace base
}  // namespace cyber
}  // namespace apollo

#endif  // CYBER_BASE_THREAD_POOL_H_
