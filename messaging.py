import asyncio
import json
from typing import Dict, Callable, List
from datetime import datetime
import threading
import queue
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MessageQueue:
    def __init__(self):
        self.queue = queue.Queue()
        self.subscribers: Dict[str, List[Callable]] = {}
        self.running = False
        self.worker_thread = None

    def start(self):
        self.running = True
        self.worker_thread = threading.Thread(target=self._process_messages, daemon=True)
        self.worker_thread.start()
        logger.info("Serviço de mensageria iniciado")

    def stop(self):
        self.running = False
        if self.worker_thread:
            self.worker_thread.join()
        logger.info("Serviço de mensageria parado")

    def publish(self, topic: str, message: Dict):
        """Publica uma mensagem em um tópico"""
        msg = {
            "topic": topic,
            "payload": message,
            "timestamp": datetime.utcnow().isoformat()
        }
        self.queue.put(msg)
        logger.info(f"Mensagem publicada no tópico: {topic}")

    def subscribe(self, topic: str, callback: Callable):
        """Inscreve um callback em um tópico"""
        if topic not in self.subscribers:
            self.subscribers[topic] = []
        self.subscribers[topic].append(callback)
        logger.info(f"Nova inscrição no tópico: {topic}")

    def _process_messages(self):
        while self.running:
            try:
                msg = self.queue.get(timeout=1)
                topic = msg["topic"]
                
                if topic in self.subscribers:
                    for callback in self.subscribers[topic]:
                        try:
                            callback(msg["payload"])
                        except Exception as e:
                            logger.error(f"Erro ao processar mensagem: {str(e)}")
                
                self.queue.task_done()
            except queue.Empty:
                continue

# Instância global da fila
message_queue = MessageQueue()

# Tópicos disponíveis
TOPICS = {
    "FILE_UPLOADED": "file.uploaded",
    "ANALYSIS_STARTED": "analysis.started",
    "ANALYSIS_COMPLETED": "analysis.completed",
    "CHART_GENERATED": "chart.generated",
    "REPORT_GENERATED": "report.generated",
    "NOTIFICATION": "notification"
}

# Handlers padrão
def log_message_handler(message: Dict):
    logger.info(f"Evento recebido: {json.dumps(message, indent=2)}")

def notification_handler(message: Dict):
    logger.info(f"Notificação: {message.get('text', 'Sem texto')}")

# Inicializar com handlers padrão
message_queue.subscribe(TOPICS["FILE_UPLOADED"], log_message_handler)
message_queue.subscribe(TOPICS["ANALYSIS_COMPLETED"], log_message_handler)
message_queue.subscribe(TOPICS["NOTIFICATION"], notification_handler)