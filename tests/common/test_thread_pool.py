import dataclasses
import threading
import time
from concurrent.futures import CancelledError, Future
from functools import partial
from typing import Any, Callable, Generator, Generic, List, Optional, TypeVar

import pytest

from rconx.common.thread_pool import ThreadPool
from tests.support import ThreadCall

_T = TypeVar('_T')
_WAIT_TIMEOUT = 5.0


def _wait(event: threading.Event):
	assert event.wait(_WAIT_TIMEOUT), 'operation did not reach the expected state'


def _submit(pool: ThreadPool, func: Callable[[], _T]) -> 'Future[_T]':
	deadline = time.monotonic() + _WAIT_TIMEOUT
	while True:
		future = pool.try_submit(func)
		if future is not None:
			return future
		assert time.monotonic() < deadline, 'execution capacity did not become available'
		time.sleep(0.001)


class _HeldTask(Generic[_T]):
	def __init__(self, result: _T, release: threading.Event):
		self.result = result
		self.release = release
		self.entered = threading.Event()
		self.thread: Optional[threading.Thread] = None

	def __call__(self) -> _T:
		self.thread = threading.current_thread()
		self.entered.set()
		_wait(self.release)
		return self.result


class _Harness:
	def __init__(self):
		self.__pools: List[ThreadPool] = []
		self.__releases: List[threading.Event] = []
		self.__calls: List[ThreadCall] = []

	def pool(self, *, max_workers: Optional[int] = 2, worker_idle_timeout: float = 10.0) -> ThreadPool:
		pool = ThreadPool(max_workers=max_workers, worker_idle_timeout=worker_idle_timeout)
		self.__pools.append(pool)
		return pool

	def gate(self) -> threading.Event:
		event = threading.Event()
		self.__releases.append(event)
		return event

	def task(self, result: _T) -> _HeldTask[_T]:
		return _HeldTask(result, self.gate())

	def call(self, func: Callable[[], None]) -> ThreadCall:
		call = ThreadCall(func)
		self.__calls.append(call)
		return call

	def close(self):
		for event in self.__releases:
			event.set()
		for pool in self.__pools:
			self.call(pool.shutdown)
		for call in self.__calls:
			call.join()


@pytest.fixture
def harness() -> Generator[_Harness, None, None]:
	harness = _Harness()
	yield harness
	harness.close()


@pytest.mark.parametrize('max_workers', [1, 3, None])
@pytest.mark.parametrize('idle_timeout', [1, 0.5])
def test_valid_configuration(harness: _Harness, max_workers: Optional[int], idle_timeout: float):
	pool = harness.pool(max_workers=max_workers, worker_idle_timeout=idle_timeout)
	assert _submit(pool, lambda: 42).result(_WAIT_TIMEOUT) == 42


@pytest.mark.parametrize('value', [True, False, 1.0, '1', object()])
def test_invalid_worker_limit_type(value: Any):
	with pytest.raises(TypeError):
		ThreadPool(max_workers=value, worker_idle_timeout=1)


@pytest.mark.parametrize('value', [0, -1])
def test_invalid_worker_limit_value(value: int):
	with pytest.raises(ValueError):
		ThreadPool(max_workers=value, worker_idle_timeout=1)


@pytest.mark.parametrize('value', [None, True, False, '1', object()])
def test_invalid_idle_timeout_type(value: Any):
	with pytest.raises(TypeError):
		ThreadPool(max_workers=1, worker_idle_timeout=value)


@pytest.mark.parametrize('value', [0, -1, float('inf'), -float('inf'), float('nan')])
def test_invalid_idle_timeout_value(value: float):
	with pytest.raises(ValueError):
		ThreadPool(max_workers=1, worker_idle_timeout=value)


@pytest.mark.parametrize('value', [None, 42, object()])
def test_non_callable_submission(harness: _Harness, value: Any):
	pool = harness.pool()
	with pytest.raises(TypeError):
		pool.try_submit(value)


@pytest.mark.parametrize('result', [None, 42, b'result', object()])
def test_submission_runs_in_background_and_delivers_result(harness: _Harness, result: Any):
	pool = harness.pool(max_workers=1)
	task = harness.task(result)
	future = pool.try_submit(task)
	assert future is not None
	_wait(task.entered)
	assert task.thread is not threading.current_thread()
	assert not future.done()
	assert not task.release.is_set()
	task.release.set()
	assert future.result(_WAIT_TIMEOUT) is result


def test_capacity_rejection_and_recovery(harness: _Harness):
	pool = harness.pool(max_workers=2)
	tasks = [harness.task(index) for index in range(2)]
	futures = [pool.try_submit(task) for task in tasks]
	for task in tasks:
		_wait(task.entered)
	rejected_called = threading.Event()
	assert pool.try_submit(rejected_called.set) is None

	for task in tasks:
		task.release.set()
	for index, future in enumerate(futures):
		assert future is not None
		assert future.result(_WAIT_TIMEOUT) == index
	assert _submit(pool, lambda: 42).result(_WAIT_TIMEOUT) == 42
	pool.shutdown()
	assert not rejected_called.is_set()


def test_unbounded_capacity_grows_for_concurrent_tasks(harness: _Harness):
	pool = harness.pool(max_workers=None)
	tasks = [harness.task(index) for index in range(8)]
	futures = [pool.try_submit(task) for task in tasks]
	for task in tasks:
		_wait(task.entered)
	assert len({task.thread for task in tasks}) == len(tasks)
	for task in tasks:
		task.release.set()
	for index, future in enumerate(futures):
		assert future is not None
		assert future.result(_WAIT_TIMEOUT) == index


def test_workers_are_created_on_demand(harness: _Harness, monkeypatch: pytest.MonkeyPatch):
	started: List[threading.Thread] = []
	original_start = threading.Thread.start

	def record_start(thread: threading.Thread):
		started.append(thread)
		original_start(thread)

	monkeypatch.setattr(threading.Thread, 'start', record_start)
	pool = harness.pool(max_workers=4)
	assert not started
	worker = _submit(pool, threading.current_thread).result(_WAIT_TIMEOUT)
	assert started == [worker]


def test_idle_worker_is_reused(harness: _Harness):
	pool = harness.pool(max_workers=1)
	first = _submit(pool, threading.current_thread).result(_WAIT_TIMEOUT)
	second = _submit(pool, threading.current_thread).result(_WAIT_TIMEOUT)
	assert second is first


def test_idle_worker_retires_and_pool_can_grow_again(harness: _Harness):
	pool = harness.pool(max_workers=1, worker_idle_timeout=0.05)
	first = _submit(pool, threading.current_thread).result(_WAIT_TIMEOUT)
	first.join(_WAIT_TIMEOUT)
	assert not first.is_alive()
	second = _submit(pool, threading.current_thread).result(_WAIT_TIMEOUT)
	assert second is not first


def test_running_task_outlives_idle_timeout_and_resets_retention(harness: _Harness):
	idle_timeout = 0.5
	pool = harness.pool(max_workers=1, worker_idle_timeout=idle_timeout)
	worker = _submit(pool, threading.current_thread).result(_WAIT_TIMEOUT)
	task = harness.task(42)
	future = _submit(pool, task)
	_wait(task.entered)
	assert task.thread is worker
	assert not future.cancel()
	assert not task.release.wait(idle_timeout * 1.5)
	assert worker.is_alive()
	assert future.running()
	task.release.set()
	assert future.result(_WAIT_TIMEOUT) == 42
	assert _submit(pool, threading.current_thread).result(_WAIT_TIMEOUT) is worker
	worker.join(_WAIT_TIMEOUT)
	assert not worker.is_alive()


class _TaskExit(BaseException):
	pass


@pytest.mark.parametrize('error', [ValueError('task failed'), _TaskExit()])
def test_task_exception_delivery_and_worker_reuse(harness: _Harness, error: BaseException):
	pool = harness.pool(max_workers=1)
	worker = _submit(pool, threading.current_thread).result(_WAIT_TIMEOUT)

	def fail():
		raise error

	future = _submit(pool, fail)
	assert future.exception(_WAIT_TIMEOUT) is error
	with pytest.raises(type(error)) as caught:
		future.result(_WAIT_TIMEOUT)
	assert caught.value is error
	assert _submit(pool, threading.current_thread).result(_WAIT_TIMEOUT) is worker


@pytest.mark.parametrize('fail', [False, True])
def test_future_completion_notification(harness: _Harness, fail: bool):
	pool = harness.pool(max_workers=1)
	gate = harness.gate()
	completed = threading.Event()
	observed: List[Future[int]] = []
	error = ValueError('task failed')

	def work() -> int:
		_wait(gate)
		if fail:
			raise error
		return 42

	def notify(future: 'Future[int]'):
		observed.append(future)
		completed.set()

	future = _submit(pool, work)
	future.add_done_callback(notify)
	gate.set()
	_wait(completed)
	pool.shutdown()
	assert observed == [future]
	if fail:
		assert future.exception() is error
	else:
		assert future.result() == 42


def test_cancellation_before_worker_executes_task(harness: _Harness, monkeypatch: pytest.MonkeyPatch):
	pool = harness.pool(max_workers=1)
	entered = threading.Event()
	release = harness.gate()
	called = threading.Event()
	completed = threading.Event()
	original_run = threading.Thread.run

	def delayed_run(thread: threading.Thread):
		entered.set()
		_wait(release)
		original_run(thread)

	with monkeypatch.context() as patch:
		patch.setattr(threading.Thread, 'run', delayed_run)
		future = pool.try_submit(called.set)
		assert future is not None
		_wait(entered)
		future.add_done_callback(lambda _: completed.set())
		assert future.cancel()
		release.set()
	_wait(completed)
	assert _submit(pool, lambda: 42).result(_WAIT_TIMEOUT) == 42
	assert future.cancelled()
	with pytest.raises(CancelledError):
		future.result()
	assert not called.is_set()


def test_shutdown_without_wait_keeps_running_tasks(harness: _Harness):
	pool = harness.pool(max_workers=1)
	task = harness.task(42)
	future = _submit(pool, task)
	_wait(task.entered)
	stop = harness.call(lambda: pool.shutdown(wait=False))
	_wait(stop.done)
	stop.join()
	assert not future.done()
	assert task.thread is not None and task.thread.is_alive()
	with pytest.raises(RuntimeError):
		pool.try_submit(lambda: None)
	task.release.set()
	pool.shutdown()
	assert future.result() == 42
	assert not task.thread.is_alive()


def test_shutdown_waits_for_running_tasks(harness: _Harness):
	pool = harness.pool(max_workers=1)
	task = harness.task(42)
	future = _submit(pool, task)
	_wait(task.entered)
	started = threading.Event()

	def stop():
		started.set()
		pool.shutdown()

	call = harness.call(stop)
	_wait(started)
	assert not call.done.wait(0.05)
	task.release.set()
	call.join()
	assert future.result() == 42
	assert task.thread is not None and not task.thread.is_alive()


def test_shutdown_waits_for_worker_completion_callback(harness: _Harness):
	pool = harness.pool(max_workers=1)
	task = harness.task(42)
	callback_entered = threading.Event()
	callback_release = harness.gate()
	callback_finished = threading.Event()
	callback_errors: List[BaseException] = []

	def callback(future: 'Future[int]'):
		callback_entered.set()
		try:
			_wait(callback_release)
		except BaseException as error:
			callback_errors.append(error)
		finally:
			callback_finished.set()

	future = _submit(pool, task)
	future.add_done_callback(callback)
	task.release.set()
	_wait(callback_entered)
	assert future.done()
	stop_started = threading.Event()

	def stop():
		stop_started.set()
		pool.shutdown()

	stop_call = harness.call(stop)
	_wait(stop_started)
	assert not stop_call.done.wait(0.05)
	callback_release.set()
	stop_call.join()
	assert callback_finished.is_set()
	assert not callback_errors
	assert task.thread is not None and not task.thread.is_alive()


def test_shutdown_wakes_workers_without_waiting_for_idle_expiry(harness: _Harness):
	pool = harness.pool(max_workers=1, worker_idle_timeout=30)
	worker = _submit(pool, threading.current_thread).result(_WAIT_TIMEOUT)
	stop = harness.call(pool.shutdown)
	stop.join()
	assert not worker.is_alive()


@pytest.mark.parametrize('used', [False, True])
def test_repeated_shutdown_and_submission_after_stop(harness: _Harness, used: bool):
	pool = harness.pool(max_workers=1)
	if used:
		assert _submit(pool, lambda: 42).result(_WAIT_TIMEOUT) == 42
	pool.shutdown(wait=False)
	pool.shutdown(wait=False)
	pool.shutdown()
	pool.shutdown()
	with pytest.raises(RuntimeError):
		pool.try_submit(lambda: None)


def test_accepted_task_finishes_after_shutdown_before_execution(harness: _Harness, monkeypatch: pytest.MonkeyPatch):
	pool = harness.pool(max_workers=1)
	entered = threading.Event()
	release = harness.gate()
	original_run = threading.Thread.run

	def delayed_run(thread: threading.Thread):
		entered.set()
		_wait(release)
		original_run(thread)

	with monkeypatch.context() as patch:
		patch.setattr(threading.Thread, 'run', delayed_run)
		future = pool.try_submit(lambda: 42)
		assert future is not None
		_wait(entered)
		pool.shutdown(wait=False)
		assert not future.done()
		release.set()
	pool.shutdown()
	assert future.result() == 42


def test_worker_can_request_shutdown_without_waiting(harness: _Harness):
	pool = harness.pool(max_workers=1)
	future = _submit(pool, lambda: pool.shutdown(wait=False))
	assert future.result(_WAIT_TIMEOUT) is None
	pool.shutdown()
	with pytest.raises(RuntimeError):
		pool.try_submit(lambda: None)


def test_worker_cannot_wait_for_its_own_shutdown(harness: _Harness):
	pool = harness.pool(max_workers=1)
	future = _submit(pool, pool.shutdown)
	with pytest.raises(RuntimeError):
		future.result(_WAIT_TIMEOUT)
	pool.shutdown()


@dataclasses.dataclass(frozen=True)
class _Submission:
	token: int
	future: 'Optional[Future[int]]'


def test_concurrent_submissions_respect_capacity_and_execute_once(harness: _Harness):
	capacity = 3
	count = 9
	pool = harness.pool(max_workers=capacity)
	barrier = threading.Barrier(count + 1)
	release = harness.gate()
	lock = threading.Lock()
	submissions: List[_Submission] = []
	executed: List[int] = []

	def work(token: int) -> int:
		with lock:
			executed.append(token)
		_wait(release)
		return token

	def submit(token: int):
		barrier.wait(_WAIT_TIMEOUT)
		future = pool.try_submit(lambda: work(token))
		with lock:
			submissions.append(_Submission(token, future))

	calls = [harness.call(partial(submit, token)) for token in range(count)]
	barrier.wait(_WAIT_TIMEOUT)
	for call in calls:
		call.join()
	accepted = [submission for submission in submissions if submission.future is not None]
	assert len(accepted) == capacity
	assert len(submissions) == count
	release.set()
	pool.shutdown()
	assert len(executed) == capacity
	assert set(executed) == {submission.token for submission in accepted}
	for submission in accepted:
		assert submission.future is not None
		assert submission.future.result() == submission.token


def test_concurrent_submission_idle_retirement_and_shutdown(harness: _Harness):
	pool = harness.pool(max_workers=3, worker_idle_timeout=0.01)
	barrier = threading.Barrier(3)
	lock = threading.Lock()
	submissions: List[_Submission] = []
	executed: List[int] = []
	stopped = harness.gate()
	progress = [threading.Event(), threading.Event()]

	def work(token: int) -> int:
		with lock:
			executed.append(token)
		return token

	def submit(index: int):
		barrier.wait(_WAIT_TIMEOUT)
		for number in range(15):
			token = index * 100 + number
			try:
				future = pool.try_submit(partial(work, token))
			except RuntimeError:
				assert stopped.wait(_WAIT_TIMEOUT)
				break
			with lock:
				submissions.append(_Submission(token, future))
			if number == 4:
				progress[index].set()
			stopped.wait(0.005 if number % 2 == 0 else 0.015)

	def stop():
		for event in progress:
			_wait(event)
		pool.shutdown(wait=False)
		stopped.set()

	calls = [harness.call(partial(submit, index)) for index in range(2)]
	calls.append(harness.call(stop))
	barrier.wait(_WAIT_TIMEOUT)
	for call in calls:
		call.join()
	pool.shutdown()
	accepted = [submission for submission in submissions if submission.future is not None]
	assert len(executed) == len(accepted)
	assert set(executed) == {submission.token for submission in accepted}
	for submission in accepted:
		assert submission.future is not None
		assert submission.future.result() == submission.token
	with pytest.raises(RuntimeError):
		pool.try_submit(lambda: None)


def test_concurrent_shutdown_callers_complete(harness: _Harness):
	pool = harness.pool(max_workers=2)
	tasks = [harness.task(index) for index in range(2)]
	futures = [_submit(pool, task) for task in tasks]
	for task in tasks:
		_wait(task.entered)
	barrier = threading.Barrier(5)

	def stop(wait: bool):
		barrier.wait(_WAIT_TIMEOUT)
		pool.shutdown(wait=wait)

	calls = [harness.call(partial(stop, wait)) for wait in [False, False, True, True]]
	barrier.wait(_WAIT_TIMEOUT)
	for call in calls[:2]:
		call.join()
	assert all(not future.done() for future in futures)
	for task in tasks:
		task.release.set()
	for call in calls:
		call.join()
	for index, future in enumerate(futures):
		assert future.result() == index
		worker = tasks[index].thread
		assert worker is not None and not worker.is_alive()
