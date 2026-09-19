'use strict';

/**
 * 高级异步任务队列
 * - 优先级调度
 * - 并发控制
 * - 指数退避重试
 * - 超时取消（AbortSignal）
 */

class TaskCancelledError extends Error {
  constructor(message = 'Task was cancelled') {
    super(message);
    this.name = 'TaskCancelledError';
  }
}

class TaskTimeoutError extends Error {
  constructor(message = 'Task timed out') {
    super(message);
    this.name = 'TaskTimeoutError';
  }
}

class AsyncTaskQueue {
  /**
   * @param {object}   options
   * @param {number}   options.concurrency      最大并发数（默认 4）
   * @param {number}   options.maxRetries       最大重试次数（默认 3）
   * @param {number}   options.retryBaseDelay   退避基础毫秒数（默认 200）
   * @param {number}   options.retryMaxDelay    退避上限毫秒数（默认 10000）
   * @param {number}   options.retryFactor      退避指数因子（默认 2）
   * @param {boolean}  options.retryJitter      是否加入随机抖动（默认 true）
   * @param {number}   options.defaultTimeout   任务默认超时毫秒数（默认 0 = 不超时）
   * @param {boolean}  options.autoStart        入队后是否自动运行（默认 true）
   */
  constructor(options = {}) {
    const {
      concurrency = 4,
      maxRetries = 3,
      retryBaseDelay = 200,
      retryMaxDelay = 10000,
      retryFactor = 2,
      retryJitter = true,
      defaultTimeout = 0,
      autoStart = true,
    } = options;

    if (concurrency < 1) throw new RangeError('concurrency must be >= 1');

    this.concurrency = concurrency;
    this.maxRetries = maxRetries;
    this.retryBaseDelay = retryBaseDelay;
    this.retryMaxDelay = retryMaxDelay;
    this.retryFactor = retryFactor;
    this.retryJitter = retryJitter;
    this.defaultTimeout = defaultTimeout;
    this.autoStart = autoStart;

    this._queue = [];          // 最小堆（按 priority）
    this._running = 0;
    this._retrying = 0;        // 退避等待中的任务数
    this._paused = !autoStart;
    this._seq = 0;             // 同优先级时的入队序号，保证 FIFO
    this._listeners = new Map();
    this._drainResolvers = [];
    this._idleResolvers = [];
    this._totalEnqueued = 0;
    this._totalSettled = 0;
  }

  /* ---------------- 事件 ---------------- */

  on(event, handler) {
    if (!this._listeners.has(event)) this._listeners.set(event, new Set());
    this._listeners.get(event).add(handler);
    return () => this.off(event, handler);
  }

  off(event, handler) {
    this._listeners.get(event)?.delete(handler);
  }

  _emit(event, payload) {
    for (const handler of this._listeners.get(event) ?? []) {
      try { handler(payload); } catch { /* 忽略监听器异常 */ }
    }
  }

  /* ---------------- 核心 API ---------------- */

  /**
   * 添加任务。
   * @param {Function} fn  任务函数，签名: (ctx) => Promise<any>
   *                       ctx = { signal, attempt, retriesLeft }
   * @param {object}  options
   * @param {number}  options.priority   优先级，数值越小越先执行（默认 0）
   * @param {number}  options.maxRetries 覆盖队列级重试次数
   * @param {number}  options.timeout    覆盖队列级超时（毫秒，0 = 不超时）
   * @param {AbortSignal} options.signal 外部取消信号
   * @param {any}     options.meta       附加元数据，随事件一起传递
   * @returns {Promise<any>} 任务最终结果
   */
  add(fn, options = {}) {
    if (typeof fn !== 'function') throw new TypeError('task must be a function');

    const {
      priority = 0,
      maxRetries = this.maxRetries,
      timeout = this.defaultTimeout,
      signal: externalSignal,
      meta,
    } = options;

    return new Promise((resolve, reject) => {
      const task = {
        id: ++this._seq,
        fn,
        priority,
        maxRetries,
        timeout,
        externalSignal,
        meta,
        attempt: 0,
        resolve,
        reject,
        cancelled: false,
        abortController: null,
      };

      if (externalSignal?.aborted) {
        task.cancelled = true;
        reject(new TaskCancelledError());
        return;
      }

      // 排队期间的外部取消：标记后由 _pump 惰性清除
      if (externalSignal) {
        externalSignal.addEventListener('abort', () => { task.cancelled = true; }, { once: true });
      }

      this._totalEnqueued++;
      this._heapPush(task);
      this._emit('enqueued', { task });
      this._pump();
    });
  }

  /** 批量添加，返回 Promise.all 风格结果 */
  addAll(fns, options = {}) {
    return Promise.all(fns.map((fn) => this.add(fn, options)));
  }

  /** 批量添加，返回 Promise.allSettled 风格结果 */
  addAllSettled(fns, options = {}) {
    return Promise.allSettled(fns.map((fn) => this.add(fn, options)));
  }

  /** 暂停调度（运行中的任务不受影响） */
  pause() { this._paused = true; }

  /** 恢复调度 */
  resume() {
    this._paused = false;
    this._pump();
  }

  /** 清空等待队列，等待中的任务以 TaskCancelledError 拒绝 */
  clear() {
    let task;
    while ((task = this._heapPop())) {
      task.cancelled = true;
      this._settle(task);
      task.reject(new TaskCancelledError('Queue cleared'));
    }
    this._checkDrain();
  }

  /** 等待队列清空且所有运行中任务完成 */
  onIdle() {
    if (this._queue.length === 0 && this._running === 0 && this._retrying === 0) {
      return Promise.resolve();
    }
    return new Promise((resolve) => this._idleResolvers.push(resolve));
  }

  /** 等待等待队列清空（运行中的任务可能仍在执行） */
  onDrain() {
    if (this._queue.length === 0 && this._retrying === 0) return Promise.resolve();
    return new Promise((resolve) => this._drainResolvers.push(resolve));
  }

  get size() { return this._queue.length; }
  get pending() { return this._running; }
  get isPaused() { return this._paused; }

  stats() {
    return {
      queued: this._queue.length,
      running: this._running,
      totalEnqueued: this._totalEnqueued,
      totalSettled: this._totalSettled,
      paused: this._paused,
    };
  }

  /* ---------------- 调度 ---------------- */

  _pump() {
    while (!this._paused && this._running < this.concurrency && this._queue.length > 0) {
      const task = this._heapPop();
      if (task.cancelled) {
        this._emit('cancelled', { task });
        this._settle(task);
        task.reject(new TaskCancelledError());
        continue;
      }
      this._run(task);
    }
    this._checkDrain();
  }

  async _run(task) {
    this._running++;
    task.attempt++;
    this._emit('started', { task });

    const controller = new AbortController();
    task.abortController = controller;

    // 外部信号联动取消
    let onExternalAbort;
    if (task.externalSignal) {
      onExternalAbort = () => controller.abort(new TaskCancelledError());
      task.externalSignal.addEventListener('abort', onExternalAbort, { once: true });
    }

    // 超时控制
    let timeoutId;
    if (task.timeout > 0) {
      timeoutId = setTimeout(() => {
        controller.abort(new TaskTimeoutError(`Task timed out after ${task.timeout}ms`));
      }, task.timeout);
    }

    const ctx = {
      signal: controller.signal,
      attempt: task.attempt,
      retriesLeft: task.maxRetries - (task.attempt - 1),
    };

    try {
      // 与取消信号竞速：即使任务忽略 signal，超时/取消也会立即生效并释放并发槽
      const abortPromise = new Promise((_, reject) => {
        controller.signal.addEventListener(
          'abort',
          () => reject(controller.signal.reason ?? new TaskCancelledError()),
          { once: true }
        );
      });
      const result = await Promise.race([Promise.resolve().then(() => task.fn(ctx)), abortPromise]);
      this._finish(task, null, result);
    } catch (error) {
      this._finish(task, error);
    } finally {
      if (timeoutId) clearTimeout(timeoutId);
      if (task.externalSignal && onExternalAbort) {
        task.externalSignal.removeEventListener('abort', onExternalAbort);
      }
      this._running--;
      this._pump();
      this._checkIdle();
    }
  }

  _finish(task, error, result) {
    if (error) {
      const isCancel = error instanceof TaskCancelledError || task.cancelled;
      const canRetry =
        !isCancel &&
        !(error instanceof TaskTimeoutError && task.externalSignal?.aborted) &&
        task.attempt <= task.maxRetries;

      if (canRetry) {
        const delay = this._backoffDelay(task.attempt);
        this._emit('retry', { task, error, attempt: task.attempt, delay });
        this._retrying++;
        setTimeout(() => {
          this._retrying--;
          if (task.cancelled) {
            this._settle(task);
            task.reject(new TaskCancelledError());
          } else {
            this._heapPush(task);
            this._pump();
          }
          this._checkDrain();
          this._checkIdle();
        }, delay);
        return;
      }

      this._emit(isCancel ? 'cancelled' : 'failed', { task, error, attempts: task.attempt });
      this._settle(task);
      task.reject(error);
      return;
    }

    this._emit('completed', { task, result, attempts: task.attempt });
    this._settle(task);
    task.resolve(result);
  }

  _settle(task) {
    this._totalSettled++;
  }

  _backoffDelay(attempt) {
    let delay = this.retryBaseDelay * Math.pow(this.retryFactor, attempt - 1);
    if (this.retryJitter) delay = delay * (0.5 + Math.random() * 0.5);
    return Math.min(delay, this.retryMaxDelay);
  }

  _checkDrain() {
    if (this._queue.length === 0 && this._retrying === 0 && this._drainResolvers.length) {
      const resolvers = this._drainResolvers.splice(0);
      resolvers.forEach((resolve) => resolve());
    }
  }

  _checkIdle() {
    if (
      this._queue.length === 0 &&
      this._running === 0 &&
      this._retrying === 0 &&
      this._idleResolvers.length
    ) {
      const resolvers = this._idleResolvers.splice(0);
      resolvers.forEach((resolve) => resolve());
    }
  }

  /* ---------------- 优先级最小堆 ---------------- */

  _heapPush(task) {
    const heap = this._queue;
    heap.push(task);
    let i = heap.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (this._compare(heap[i], heap[parent]) < 0) {
        [heap[i], heap[parent]] = [heap[parent], heap[i]];
        i = parent;
      } else break;
    }
  }

  _heapPop() {
    const heap = this._queue;
    if (heap.length === 0) return undefined;
    const top = heap[0];
    const last = heap.pop();
    if (heap.length > 0) {
      heap[0] = last;
      let i = 0;
      for (;;) {
        const left = i * 2 + 1;
        const right = left + 1;
        let smallest = i;
        if (left < heap.length && this._compare(heap[left], heap[smallest]) < 0) smallest = left;
        if (right < heap.length && this._compare(heap[right], heap[smallest]) < 0) smallest = right;
        if (smallest === i) break;
        [heap[i], heap[smallest]] = [heap[smallest], heap[i]];
        i = smallest;
      }
    }
    return top;
  }

  _compare(a, b) {
    if (a.priority !== b.priority) return a.priority - b.priority;
    return a.id - b.id; // 同优先级 FIFO
  }
}

module.exports = { AsyncTaskQueue, TaskCancelledError, TaskTimeoutError };

/* ================= 调用示例 ================= */
if (require.main === module) {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  (async () => {
    const queue = new AsyncTaskQueue({
      concurrency: 2,        // 最多同时跑 2 个任务
      maxRetries: 3,         // 最多重试 3 次
      retryBaseDelay: 100,   // 退避: 100ms, 200ms, 400ms ...
      defaultTimeout: 2000,  // 单任务默认 2s 超时
    });

    queue.on('retry',     ({ task, attempt, delay }) =>
      console.log(`[retry]     任务#${task.id} 第${attempt}次失败，${delay | 0}ms 后重试`));
    queue.on('completed', ({ task, result }) =>
      console.log(`[completed] 任务#${task.id} =>`, result));
    queue.on('failed',    ({ task, error }) =>
      console.log(`[failed]    任务#${task.id}:`, error.message));
    queue.on('cancelled', ({ task }) =>
      console.log(`[cancelled] 任务#${task.id}`));

    // 1. 优先级调度：priority 越小越先执行
    queue.add(async () => { await sleep(300); return '低优先级'; }, { priority: 10 });
    queue.add(async () => { await sleep(100); return '高优先级'; }, { priority: 1 });

    // 2. 失败自动重试（指数退避）
    let flakyCount = 0;
    queue.add(async () => {
      flakyCount++;
      if (flakyCount < 3) throw new Error('模拟不稳定失败');
      return `第 ${flakyCount} 次尝试成功`;
    }, { priority: 5 });

    // 3. 超时取消：任务内部需响应 ctx.signal
    queue.add(async ({ signal }) => {
      for (let i = 0; i < 20; i++) {
        if (signal.aborted) throw signal.reason ?? new TaskCancelledError();
        await sleep(200);
      }
      return '不该到达这里';
    }, { timeout: 500 }).catch((e) => console.log('[timeout]', e.message));

    // 4. 外部 AbortSignal 取消
    const controller = new AbortController();
    queue.add(async () => { await sleep(5000); return '不会执行完'; },
      { signal: controller.signal }).catch((e) => console.log('[abort]', e.message));
    setTimeout(() => controller.abort(), 300);

    // 5. 批量任务
    const results = await queue.addAllSettled(
      [1, 2, 3, 4, 5].map((n) => async () => { await sleep(50); return n * 10; }),
      { priority: 8 }
    );
    console.log('[batch]', results.map((r) => r.value ?? r.reason?.message));

    await queue.onIdle();
    console.log('[idle] 全部完成', queue.stats());
  })();
}
