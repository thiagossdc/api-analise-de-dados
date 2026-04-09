import redis
from typing import Any, Optional
from datetime import timedelta
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CacheManager:
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, password: Optional[str] = None):
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
        )
        logger.info("Cache Redis inicializado")

    def set(self, key: str, value: Any, ttl: Optional[timedelta] = None):
        """Armazena um valor no cache com tempo de expiração opcional"""
        try:
            serialized_value = json.dumps(value)
            if ttl:
                self.client.setex(key, int(ttl.total_seconds()), serialized_value)
            else:
                self.client.set(key, serialized_value)
            logger.info(f"Valor armazenado no cache: {key}")
            return True
        except Exception as e:
            logger.error(f"Erro ao armazenar no cache: {str(e)}")
            return False

    def get(self, key: str) -> Optional[Any]:
        """Recupera um valor do cache"""
        try:
            value = self.client.get(key)
            if value:
                logger.info(f"Valor recuperado do cache: {key}")
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"Erro ao recuperar do cache: {str(e)}")
            return None

    def delete(self, key: str):
        """Remove um valor do cache"""
        try:
            self.client.delete(key)
            logger.info(f"Valor removido do cache: {key}")
            return True
        except Exception as e:
            logger.error(f"Erro ao remover do cache: {str(e)}")
            return False

    def exists(self, key: str) -> bool:
        """Verifica se uma chave existe no cache"""
        try:
            return self.client.exists(key) == 1
        except Exception as e:
            logger.error(f"Erro ao verificar existência no cache: {str(e)}")
            return False

# Instância global do cache
cache_manager = CacheManager()

# Decorador para cache de funções
def cached(ttl: timedelta):
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Cria uma chave única baseada nos argumentos da função
            key = f"{func.__name__}:{hash(json.dumps({'args': args, 'kwargs': kwargs}))}"
            
            # Verifica se o valor está no cache
            cached_value = cache_manager.get(key)
            if cached_value is not None:
                return cached_value

            # Se não estiver no cache, executa a função
            result = func(*args, **kwargs)

            # Armazena o resultado no cache
            cache_manager.set(key, result, ttl)

            return result
        return wrapper
    return decorator