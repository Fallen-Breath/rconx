import dataclasses
import math
import threading
import time
from concurrent.futures import Future
from typing import Any, Callable, Generic, Optional, Set, TypeVar


_ResultT = TypeVar('_ResultT')


@dataclasses.dataclass(frozen=True)
class _Task(Generic[_ResultT]):
	func: Callable[[], _ResultT]
	future: 'Future[_ResultT]'

	def run(self):
		if not self.future.set_running_or_notify_cancel():
			return
		try:
			result = self.func()
		except BaseException as error:
			self.future.set_exception(error)
		else:
			self.future.set_result(result)


class _Worker:
	def __init__(self, task: _Task[Any], condition: threading.Condition, target: Callable[['_Worker'], None]):
		self.task: Optional[_Task[Any]] = task
		self.condition = condition
		self.thread = threading.Thread(target=target, args=(self,), daemon=False)


class ThreadPool:
	"""
	Internal pool with on-demand workers, reuse and idle retirement
	"""

	def __init__(self, *, max_workers: Optional[int], worker_idle_timeout: float):
		"""
		Create an initially empty pool

		:keyword max_workers: Positive worker limit, or ``None`` for unbounded growth
		:keyword worker_idle_timeout: Finite positive idle retention time in seconds
		"""
		if max_workers is not None:
			if isinstance(max_workers, bool) or not isinstance(max_workers, int):
				raise TypeError('max_workers must be an integer or None')
			if max_workers <= 0:
				raise ValueError('max_workers must be positive')
		if isinstance(worker_idle_timeout, bool) or not isinstance(worker_idle_timeout, (int, float)):
			raise TypeError('worker_idle_timeout must be a number')
		if not math.isfinite(worker_idle_timeout) or worker_idle_timeout <= 0:
			raise ValueError('worker_idle_timeout must be finite and positive')

		self.__max_workers = max_workers
		self.__worker_idle_timeout = float(worker_idle_timeout)
		self.__lock = threading.Lock()
		self.__workers: Set[_Worker] = set()
		self.__idle_workers: Set[_Worker] = set()
		# Keep retiring threads available to shutdown until their actual exit.
		self.__retired_threads: Set[threading.Thread] = set()
		self.__stopping = False

	def try_submit(self, func: Callable[[], _ResultT]) -> 'Optional[Future[_ResultT]]':
		"""
		Accept a no-argument task without waiting for execution capacity

		The caller handles task results and exceptions through the future.
		Future completion does not imply worker availability or thread exit.

		:param func: Callable to execute in a worker
		:return: Task future, or ``None`` when capacity is unavailable; a rejected task is never executed
		:raises RuntimeError: If the pool has stopped accepting tasks
		"""
		if not callable(func):
			raise TypeError('func must be callable')
		with self.__lock:
			if self.__stopping:
				raise RuntimeError('thread pool has been shut down')
			self.__discard_finished_threads()
			worker_count = len(self.__workers) + len(self.__retired_threads)
			if not self.__idle_workers and self.__max_workers is not None and worker_count >= self.__max_workers:
				return None

			task: _Task[_ResultT] = _Task(func, Future())
			if self.__idle_workers:
				worker = self.__idle_workers.pop()
				worker.task = task
				worker.condition.notify()
			else:
				worker = _Worker(task, threading.Condition(self.__lock), self.__run_worker)
				worker.thread.start()
				self.__workers.add(worker)
		return task.future

	def shutdown(self, *, wait: bool = True):
		"""
		Stop accepting tasks without cancelling accepted work; calls may be repeated

		:keyword wait: Wait for tasks and all worker threads to exit; use ``False`` inside a worker
		:raises RuntimeError: If a worker requests waiting for its own exit
		"""
		with self.__lock:
			self.__discard_finished_threads()
			threads = self.__retired_threads | {worker.thread for worker in self.__workers}
			if wait and threading.current_thread() in threads:
				raise RuntimeError('a pool worker cannot wait for its own shutdown')
			self.__stopping = True
			for worker in self.__workers:
				worker.condition.notify()
		if wait:
			for thread in threads:
				thread.join()
			with self.__lock:
				self.__retired_threads.difference_update(threads)

	def __take_task(self, worker: _Worker) -> Optional[_Task[Any]]:
		with worker.condition:
			if worker.task is not None or self.__stopping:
				return worker.task

			deadline = time.monotonic() + self.__worker_idle_timeout
			self.__idle_workers.add(worker)
			try:
				while worker.task is None and not self.__stopping:
					remaining = deadline - time.monotonic()
					if remaining <= 0:
						return None
					worker.condition.wait(min(remaining, threading.TIMEOUT_MAX))
				return worker.task
			finally:
				self.__idle_workers.discard(worker)

	def __run_worker(self, worker: _Worker):
		error: Optional[BaseException] = None
		try:
			while True:
				task = self.__take_task(worker)
				if task is None:
					break
				task.run()
				with self.__lock:
					worker.task = None
				del task
		except BaseException as err:
			error = err
		finally:
			with self.__lock:
				unfinished = worker.task
				worker.task = None
				self.__idle_workers.discard(worker)
				self.__workers.discard(worker)
				self.__discard_finished_threads()
				self.__retired_threads.add(worker.thread)
			if error is not None:
				if unfinished is not None and not unfinished.future.done():
					unfinished.future.set_exception(error)

	def __discard_finished_threads(self):
		self.__retired_threads = {thread for thread in self.__retired_threads if thread.is_alive()}
