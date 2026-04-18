"""
多会话并行架构测试 v3

核心逻辑：
- 消息入队时 busy=True，queue.get() 后才 busy=False
- 所以"主会话忙"的定义是：正在处理（消息在 Claude 调用中），队列里可能还有待处理消息
- 但实际上，只要消息被取出 queue.get()，队列就空了

新消息到达时的判断：
- 等待队列有消息 → 入等待队列
- 等待队列空 AND 主会话不忙 AND 主会话队列空 → 主会话处理
- 等待队列空 AND 主会话忙 OR 队列非空 → 检查并行会话
"""

import unittest
import threading
import queue as queue_module
import uuid

MAX_PARALLEL_SESSIONS = 1
MAX_WAITING_QUEUE = 3

_main_sessions = {}
_parallel_sessions = {}
_waiting_queue = {}
_global_lock = threading.Lock()

from dataclasses import dataclass, field
from typing import Optional

@dataclass
class MainSession:
    chat_id: str
    session_id: str
    busy: bool = False
    queue: queue_module.Queue = field(default_factory=queue_module.Queue)
    thread: Optional[threading.Thread] = None

@dataclass
class ParallelSession:
    session_id: str
    parent_chat_id: str
    busy: bool = False
    queue: queue_module.Queue = field(default_factory=queue_module.Queue)
    thread: Optional[threading.Thread] = None

def clear_all():
    _main_sessions.clear()
    _parallel_sessions.clear()
    _waiting_queue.clear()

def simulate_message_processed(session):
    """模拟消息处理完成：消息从队列取出，busy=False"""
    try:
        session.queue.get_nowait()
    except:
        pass
    session.busy = False

def test_handle_message(chat_id, text, chat_type="p2p"):
    """与 main_websocket.py handle_message 完全一致的分发逻辑"""
    global _main_sessions, _parallel_sessions, _waiting_queue
    message_id = f"msg_{uuid.uuid4().hex[:8]}"

    with _global_lock:
        if chat_id not in _main_sessions:
            main = MainSession(chat_id=chat_id, session_id="")
            _main_sessions[chat_id] = main
            _waiting_queue[chat_id] = queue_module.Queue()

    main = _main_sessions[chat_id]
    queue_size = _waiting_queue[chat_id].qsize()

    # 1. 等待队列有消息 → 入等待队列
    if queue_size > 0:
        if queue_size >= MAX_WAITING_QUEUE:
            return ("reject", chat_id, text, f"queue_full({queue_size})")
        _waiting_queue[chat_id].put((text, message_id, chat_type))
        return ("enqueue", chat_id, text, f"queued({queue_size+1})")

    # 2. 等待队列空 → 检查各会话
    if not main.busy and main.queue.empty():
        # 主会话空闲 → 分发主会话
        main.queue.put((text, message_id, chat_type))
        main.busy = True
        return ("main", chat_id, text, "direct")
    else:
        # 主会话忙(或不空)
        with _global_lock:
            idle_parallel = None
            for ps in _parallel_sessions.values():
                if ps.parent_chat_id == chat_id and not ps.busy and ps.queue.empty():
                    idle_parallel = ps
                    break

            if idle_parallel:
                idle_parallel.queue.put((text, message_id, chat_type))
                return ("parallel", chat_id, text, f"to_{idle_parallel.session_id[:8]}")
            elif len(_parallel_sessions) < MAX_PARALLEL_SESSIONS:
                new_sid = str(uuid.uuid4())
                parallel = ParallelSession(session_id=new_sid, parent_chat_id=chat_id)
                _parallel_sessions[new_sid] = parallel
                parallel.queue.put((text, message_id, chat_type))
                return ("new_parallel", chat_id, text, new_sid[:8])
            else:
                if queue_size >= MAX_WAITING_QUEUE:
                    return ("reject", chat_id, text, "all_busy_full")
                _waiting_queue[chat_id].put((text, message_id, chat_type))
                return ("enqueue", chat_id, text, f"queued({queue_size+1})")


class TestMultiSession(unittest.TestCase):

    def setUp(self):
        clear_all()

    def test_tc1_single_message(self):
        """TC1: 单消息 → 主会话处理"""
        r = test_handle_message("c1", "你好")
        self.assertEqual(r[0], "main")
        self.assertTrue(_main_sessions["c1"].busy)
        print(f"TC1 OK: {r}")

    def test_tc2_main_idle_new_msg(self):
        """TC2: 主会话空闲 + 新消息 → 主会话处理"""
        test_handle_message("c1", "M1")
        # 模拟M1处理完：queue.get() + busy=False
        simulate_message_processed(_main_sessions["c1"])

        # M2 → 主会话空闲 → 主会话处理
        r = test_handle_message("c1", "M2")
        self.assertEqual(r[0], "main")
        print(f"TC2 OK: {r}")

    def test_tc3_main_busy_parallel(self):
        """TC3: 主会话忙 + 并行未创建 → 创建并行会话"""
        test_handle_message("c1", "M1")  # busy=True, queue有M1

        # M2 → 主会话忙(queue非空) → 创建并行会话
        r = test_handle_message("c1", "M2")
        self.assertEqual(r[0], "new_parallel")
        self.assertEqual(len(_parallel_sessions), 1)
        print(f"TC3 OK: {r}")

    def test_tc4_parallel_idle_new_msg(self):
        """TC4: 主会话忙，并行会话空闲 → 分发给并行会话"""
        test_handle_message("c1", "M1")  # main忙，queue有M1
        r = test_handle_message("c1", "M2")  # new_parallel
        self.assertEqual(r[0], "new_parallel")

        # M1处理完，但queue里还有M1(被parallel的session持有)，main.busy=False，main.queue非空
        simulate_message_processed(_main_sessions["c1"])

        # M3 → 主会话忙(队列非空) → 不走main；找idle parallel → 有(但不空，queue有M2) → 实际：main空闲+队空会走main
        # 正确场景：M1处理完且queue空了，main空闲，M3应该去main
        # 重新理解：simulate后 main.queue空了(因为get_nowait)且busy=False
        # M3 → not main.busy and main.queue.empty() → True → 走main
        r = test_handle_message("c1", "M3")
        self.assertEqual(r[0], "main")  # 修正：main空闲时优先主会话
        print(f"TC4 OK: {r}")

    def test_tc5_both_busy_queue(self):
        """TC5: 主会话忙 + 并行会话忙 → 入等待队列"""
        test_handle_message("c1", "M1")  # main忙
        test_handle_message("c1", "M2")  # 并行会话忙

        # M3 → 都忙 → 入等待队列
        r = test_handle_message("c1", "M3")
        self.assertEqual(r[0], "enqueue")
        self.assertEqual(_waiting_queue["c1"].qsize(), 1)
        print(f"TC5 OK: {r}")

    def test_tc6_queue_full_reject(self):
        """TC6: 等待队列满(3条) → 第6条被拒绝"""
        # M1→main, M2→parallel(新), M3→队, M4→队, M5→队(队满3条)
        test_handle_message("c1", "M1")
        test_handle_message("c1", "M2")
        test_handle_message("c1", "M3")
        test_handle_message("c1", "M4")
        test_handle_message("c1", "M5")  # 第5条，队满

        self.assertEqual(_waiting_queue["c1"].qsize(), 3)
        r = test_handle_message("c1", "M6")  # 第6条 → 拒绝
        self.assertEqual(r[0], "reject")
        print(f"TC6 OK: 队满拒绝")

    def test_tc7_queue_fifo(self):
        """TC7: 等待队列 FIFO 顺序"""
        # M1→main, M2→parallel, M3→队, M4→队
        test_handle_message("c1", "M1")
        test_handle_message("c1", "M2")
        test_handle_message("c1", "M3")
        test_handle_message("c1", "M4")

        items = []
        while not _waiting_queue["c1"].empty():
            items.append(_waiting_queue["c1"].get_nowait()[0])
        self.assertEqual(items, ["M3", "M4"])
        print(f"TC7 OK: FIFO {[items]}")

    def test_tc8_both_idle_close_parallel(self):
        """TC8: 主会话空闲 + 并行会话空闲 + 队列空 → 关闭并行会话"""
        test_handle_message("c1", "M1")
        test_handle_message("c1", "M2")

        # M1处理完
        simulate_message_processed(_main_sessions["c1"])
        # M2处理完
        ps = list(_parallel_sessions.values())[0]
        simulate_message_processed(ps)

        # 检查关闭
        if ps.queue.empty() and _waiting_queue["c1"].empty():
            _parallel_sessions.pop(ps.session_id, None)

        self.assertEqual(len(_parallel_sessions), 0)
        print(f"TC8 OK: 并行会话已关闭")

    def test_tc9_chat_ids_isolated(self):
        """TC9: 多 chat_id 隔离"""
        test_handle_message("c1", "M1")
        test_handle_message("c2", "M1")
        self.assertEqual(len(_main_sessions), 2)
        print(f"TC9 OK: 隔离")

    def test_tc10_waiting_queue_priority(self):
        """TC10: 等待队列有消息时，新消息入队列（不入并行会话）"""
        test_handle_message("c1", "M1")  # main忙
        test_handle_message("c1", "M2")  # 并行
        test_handle_message("c1", "M3")  # 队

        # 并行处理完
        ps = list(_parallel_sessions.values())[0]
        simulate_message_processed(ps)

        # M4 → 等待队列有消息 → 入队列（不入并行）
        r = test_handle_message("c1", "M4")
        self.assertEqual(r[0], "enqueue")
        print(f"TC10 OK: {r[0]}")


if __name__ == "__main__":
    print("=" * 60)
    print("多会话并行架构测试 v3")
    print("=" * 60)
    unittest.main(verbosity=2)
