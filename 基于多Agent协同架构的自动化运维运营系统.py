import threading
import queue
import time
import random
import logging
import datetime
from typing import Dict, Any, Optional, List

# 配置日志显示格式，便于观察多Agent协同工作过程
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)


class MessageBroker:
    """
    消息代理（中介者模式）
    负责管理所有Agent的消息队列，提供注册、注销和消息投递功能。
    每个Agent拥有独立的线程安全队列，通过Broker进行异步通信。
    """
    def __init__(self):
        self._agents: Dict[str, queue.Queue] = {}
        self._lock = threading.Lock()

    def register_agent(self, agent_id: str) -> None:
        """注册Agent，为其创建消息队列"""
        with self._lock:
            if agent_id not in self._agents:
                self._agents[agent_id] = queue.Queue()
                logging.getLogger("Broker").info(f"Agent '{agent_id}' 已注册")

    def unregister_agent(self, agent_id: str) -> None:
        """注销Agent，删除其消息队列"""
        with self._lock:
            if agent_id in self._agents:
                del self._agents[agent_id]
                logging.getLogger("Broker").info(f"Agent '{agent_id}' 已注销")

    def send(self, target_agent_id: str, message: Dict[str, Any], sender_id: str = None) -> bool:
        """
        向指定Agent发送消息
        :param target_agent_id: 目标Agent的ID
        :param message: 消息内容（字典格式）
        :param sender_id: 发送者ID（用于日志追踪）
        :return: 是否发送成功
        """
        with self._lock:
            if target_agent_id in self._agents:
                # 添加元数据：时间戳、发送者
                msg_with_meta = {
                    **message,
                    "_timestamp": datetime.datetime.now().isoformat(),
                    "_sender": sender_id
                }
                self._agents[target_agent_id].put(msg_with_meta)
                logger = logging.getLogger("Broker")
                logger.debug(f"消息从 {sender_id} 发送到 {target_agent_id}: {message.get('type', 'unknown')}")
                return True
            else:
                logging.getLogger("Broker").warning(f"目标Agent '{target_agent_id}' 不存在，消息丢弃")
                return False

    def get_queue(self, agent_id: str) -> Optional[queue.Queue]:
        """获取Agent的消息队列（仅供Agent内部使用）"""
        with self._lock:
            return self._agents.get(agent_id)


class Agent(threading.Thread):
    """
    智能体基类
    每个Agent运行在独立线程中，通过消息队列接收并处理消息。
    子类需要实现 handle_message 方法定义具体行为。
    """
    def __init__(self, agent_id: str, broker: MessageBroker):
        super().__init__(name=f"Agent-{agent_id}")
        self.agent_id = agent_id
        self.broker = broker
        self._stop_event = threading.Event()
        self.logger = logging.getLogger(f"Agent.{agent_id}")

    def register(self) -> None:
        """向Broker注册自己"""
        self.broker.register_agent(self.agent_id)

    def unregister(self) -> None:
        """从Broker注销"""
        self.broker.unregister_agent(self.agent_id)

    def send_message(self, target_agent_id: str, message: Dict[str, Any]) -> bool:
        """发送消息给其他Agent（封装Broker的发送方法）"""
        return self.broker.send(target_agent_id, message, sender_id=self.agent_id)

    def stop(self) -> None:
        """停止Agent线程"""
        self._stop_event.set()

    def run(self) -> None:
        """主循环：从队列中获取消息并处理"""
        self.logger.info(f"启动运行")
        self.register()
        msg_queue = self.broker.get_queue(self.agent_id)
        if msg_queue is None:
            self.logger.error("无法获取消息队列，Agent将退出")
            return

        try:
            while not self._stop_event.is_set():
                try:
                    # 设置超时，以便定期检查停止标志
                    message = msg_queue.get(timeout=0.5)
                    self.handle_message(message)
                except queue.Empty:
                    continue
                except Exception as e:
                    self.logger.error(f"处理消息时发生错误: {e}", exc_info=True)
        finally:
            self.unregister()
            self.logger.info(f"已停止")

    def handle_message(self, message: Dict[str, Any]) -> None:
        """
        处理接收到的消息（抽象方法，子类必须实现）
        :param message: 接收到的消息字典
        """
        raise NotImplementedError("子类必须实现 handle_message 方法")


class MonitorAgent(Agent):
    """
    监控智能体
    职责：周期性地生成模拟服务器性能指标（CPU、内存使用率），
    并将数据发送给两个分析智能体（CPU分析器和内存分析器）。
    """
    def __init__(self, broker: MessageBroker, cpu_analyzer_id: str, mem_analyzer_id: str,
                 interval: int = 5):
        super().__init__("MonitorAgent", broker)
        self.cpu_analyzer_id = cpu_analyzer_id
        self.mem_analyzer_id = mem_analyzer_id
        self.interval = interval

    def _generate_metrics(self) -> Dict[str, float]:
        """模拟生成服务器性能指标（随机值，可产生异常情况）"""
        # CPU使用率：通常在20%到95%之间波动，偶尔过载
        cpu = random.uniform(20, 95)
        # 内存使用率：通常在30%到92%之间
        memory = random.uniform(30, 92)
        return {"cpu": round(cpu, 1), "memory": round(memory, 1)}

    def run(self) -> None:
        """重写run：周期性生成数据并发送给分析Agent"""
        self.logger.info(f"启动监控，采样间隔={self.interval}秒")
        self.register()
        try:
            while not self._stop_event.is_set():
                # 模拟采集数据
                metrics = self._generate_metrics()
                self.logger.info(f"采集到新指标: CPU={metrics['cpu']}%, 内存={metrics['memory']}%")

                # 构建消息并发送给两个分析Agent
                message = {
                    "type": "metrics_data",
                    "data": metrics,
                    "timestamp": datetime.datetime.now().isoformat()
                }
                self.send_message(self.cpu_analyzer_id, message)
                self.send_message(self.mem_analyzer_id, message)

                # 等待下一个采集周期
                for _ in range(self.interval * 2):  # 每0.5秒检查一次停止标志
                    if self._stop_event.is_set():
                        break
                    time.sleep(0.5)
        finally:
            self.unregister()
            self.logger.info("已停止监控")


class CPUAnalyzerAgent(Agent):
    """
    CPU分析智能体
    职责：接收监控数据，分析CPU使用率是否超过阈值（>80%），
    若超过则向协调智能体发送告警消息。
    """
    def __init__(self, broker: MessageBroker, coordinator_id: str, cpu_threshold: float = 80.0):
        super().__init__("CPUAnalyzer", broker)
        self.coordinator_id = coordinator_id
        self.cpu_threshold = cpu_threshold

    def handle_message(self, message: Dict[str, Any]) -> None:
        """处理接收到的监控数据消息"""
        if message.get("type") == "metrics_data":
            data = message.get("data", {})
            cpu_value = data.get("cpu")
            if cpu_value is not None:
                self.logger.debug(f"分析CPU数据: {cpu_value}%")
                if cpu_value > self.cpu_threshold:
                    alert_msg = {
                        "type": "cpu_alert",
                        "value": cpu_value,
                        "threshold": self.cpu_threshold,
                        "timestamp": message.get("timestamp")
                    }
                    self.send_message(self.coordinator_id, alert_msg)
                    self.logger.info(f"⚠️ CPU告警: {cpu_value}% > {self.cpu_threshold}%，已通知协调器")
                else:
                    self.logger.debug(f"CPU正常: {cpu_value}%")
        else:
            self.logger.warning(f"收到未知类型消息: {message.get('type')}")


class MemoryAnalyzerAgent(Agent):
    """
    内存分析智能体
    职责：接收监控数据，分析内存使用率是否超过阈值（>85%），
    若超过则向协调智能体发送告警消息。
    """
    def __init__(self, broker: MessageBroker, coordinator_id: str, memory_threshold: float = 85.0):
        super().__init__("MemoryAnalyzer", broker)
        self.coordinator_id = coordinator_id
        self.memory_threshold = memory_threshold

    def handle_message(self, message: Dict[str, Any]) -> None:
        if message.get("type") == "metrics_data":
            data = message.get("data", {})
            memory_value = data.get("memory")
            if memory_value is not None:
                self.logger.debug(f"分析内存数据: {memory_value}%")
                if memory_value > self.memory_threshold:
                    alert_msg = {
                        "type": "memory_alert",
                        "value": memory_value,
                        "threshold": self.memory_threshold,
                        "timestamp": message.get("timestamp")
                    }
                    self.send_message(self.coordinator_id, alert_msg)
                    self.logger.info(f"⚠️ 内存告警: {memory_value}% > {self.memory_threshold}%，已通知协调器")
                else:
                    self.logger.debug(f"内存正常: {memory_value}%")
        else:
            self.logger.warning(f"收到未知类型消息: {message.get('type')}")


class CoordinatorAgent(Agent):
    """
    协调智能体（核心决策者）
    职责：接收来自CPU和内存分析器的告警，在时间窗口内聚合两种告警，
    若同时出现（或时间窗口内连续出现），则做出自动扩容决策，并通知执行智能体。
    实现多源信息协同决策，避免单维度误报。
    """
    def __init__(self, broker: MessageBroker, executor_id: str, window_seconds: float = 30.0,
                 cooldown_seconds: float = 60.0):
        super().__init__("Coordinator", broker)
        self.executor_id = executor_id
        self.window_seconds = window_seconds      # 告警协同时间窗口
        self.cooldown_seconds = cooldown_seconds  # 扩容冷却时间，避免频繁扩容
        self.last_scale_time = 0.0                # 上次扩容时间戳
        # 存储最近的告警记录: {'cpu': timestamp, 'memory': timestamp}
        self.pending_alerts: Dict[str, float] = {}

    def _clean_expired_alerts(self, current_time: float) -> None:
        """清理超过时间窗口的告警记录"""
        expired_keys = []
        for alert_type, alert_time in self.pending_alerts.items():
            if current_time - alert_time > self.window_seconds:
                expired_keys.append(alert_type)
        for key in expired_keys:
            del self.pending_alerts[key]
            self.logger.debug(f"过期告警已清理: {key}")

    def _check_and_decide(self, current_time: float) -> bool:
        """
        检查是否同时存在CPU和内存告警（在窗口内），若是则决定扩容
        :return: 是否触发扩容
        """
        has_cpu = 'cpu' in self.pending_alerts
        has_memory = 'memory' in self.pending_alerts
        if has_cpu and has_memory:
            # 检查两个告警是否都在窗口内（已通过清理保证），满足协同条件
            self.logger.info(f"✅ 协同决策: CPU告警与内存告警同时存在，触发自动扩容流程")
            return True
        return False

    def handle_message(self, message: Dict[str, Any]) -> None:
        msg_type = message.get("type")
        current_time = time.time()

        # 处理CPU告警
        if msg_type == "cpu_alert":
            self.pending_alerts['cpu'] = current_time
            self.logger.info(f"📊 协同状态: 收到CPU告警 (当前值={message.get('value')}%)")
            self._clean_expired_alerts(current_time)
            if self._check_and_decide(current_time):
                self._trigger_scale()

        # 处理内存告警
        elif msg_type == "memory_alert":
            self.pending_alerts['memory'] = current_time
            self.logger.info(f"📊 协同状态: 收到内存告警 (当前值={message.get('value')}%)")
            self._clean_expired_alerts(current_time)
            if self._check_and_decide(current_time):
                self._trigger_scale()

        else:
            self.logger.debug(f"收到无关消息类型: {msg_type}")

    def _trigger_scale(self) -> None:
        """触发扩容动作：向执行智能体发送指令，并加入冷却机制"""
        current_time = time.time()
        if current_time - self.last_scale_time < self.cooldown_seconds:
            self.logger.info(f"⏸️ 扩容冷却中，距上次扩容仅 {current_time - self.last_scale_time:.1f} 秒，跳过本次执行")
            return

        # 清除当前告警记录，避免重复触发
        self.pending_alerts.clear()
        self.last_scale_time = current_time

        # 发送扩容指令给执行Agent
        scale_command = {
            "type": "scale_up",
            "reason": "CPU和内存同时超限告警",
            "timestamp": datetime.datetime.now().isoformat()
        }
        self.send_message(self.executor_id, scale_command)
        self.logger.info("🚀 已发送扩容指令给执行器")


class ExecutorAgent(Agent):
    """
    执行智能体
    职责：接收来自协调器的指令，执行具体的自动化运营操作（模拟扩容、记录日志等）。
    """
    def __init__(self, broker: MessageBroker):
        super().__init__("Executor", broker)
        self.action_count = 0

    def handle_message(self, message: Dict[str, Any]) -> None:
        msg_type = message.get("type")
        if msg_type == "scale_up":
            self.action_count += 1
            reason = message.get("reason", "未知原因")
            self.logger.info(f"⚙️ 执行自动扩容操作 (第{self.action_count}次) | 原因: {reason}")
            # 模拟扩容动作：在实际系统中可以是调用云API、启动容器等
            self._simulate_auto_scale()
        else:
            self.logger.warning(f"收到未知指令类型: {msg_type}")

    def _simulate_auto_scale(self) -> None:
        """模拟执行扩容动作（增加服务器实例）"""
        self.logger.info("    → 模拟: 正在增加新的服务器实例...")
        time.sleep(0.5)  # 模拟执行耗时
        self.logger.info("    → 扩容成功: 新实例已加入集群，当前运营能力提升")
        # 可以记录更多业务指标（略）


def main():
    """
    系统主流程：创建Broker和所有Agent，启动协同运营自动化系统。
    运行60秒后自动停止，展示完整的多Agent协同过程。
    """
    print("=" * 60)
    print("多Agent协同运营自动化系统 启动")
    print("场景: 服务器监控 -> 多维度分析 -> 协同决策 -> 自动扩容")
    print("=" * 60)

    # 1. 创建消息代理（中介）
    broker = MessageBroker()

    # 2. 定义Agent ID
    cpu_analyzer_id = "CPUAnalyzer"
    mem_analyzer_id = "MemoryAnalyzer"
    coordinator_id = "Coordinator"
    executor_id = "Executor"
    monitor_id = "MonitorAgent"

    # 3. 创建所有智能体实例
    monitor = MonitorAgent(
        broker=broker,
        cpu_analyzer_id=cpu_analyzer_id,
        mem_analyzer_id=mem_analyzer_id,
        interval=4  # 每4秒采集一次数据
    )
    cpu_analyzer = CPUAnalyzerAgent(broker, coordinator_id, cpu_threshold=80.0)
    mem_analyzer = MemoryAnalyzerAgent(broker, coordinator_id, memory_threshold=85.0)
    coordinator = CoordinatorAgent(broker, executor_id, window_seconds=25.0, cooldown_seconds=40.0)
    executor = ExecutorAgent(broker)

    agents: List[Agent] = [monitor, cpu_analyzer, mem_analyzer, coordinator, executor]

    # 4. 启动所有Agent线程
    for agent in agents:
        agent.start()

    # 5. 让系统运行一段时间（模拟持续运营）
    run_duration = 60  # 秒
    print(f"\n系统运行中，持续 {run_duration} 秒，观察协同决策与自动扩容...\n")
    time.sleep(run_duration)

    # 6. 停止所有Agent
    print("\n运行时间结束，正在停止所有智能体...")
    for agent in agents:
        agent.stop()
    for agent in agents:
        agent.join(timeout=2)

    print("\n多Agent协同运营自动化系统已关闭。")
    print(f"共执行自动扩容次数: {executor.action_count}")
    print("=" * 60)


if __name__ == "__main__":
    main()