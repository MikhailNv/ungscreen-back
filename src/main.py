from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pybit.unified_trading import HTTP, WebSocket as BybitWebSocket
from contextlib import asynccontextmanager
import asyncio
from collections import deque
import json
import logging
from datetime import datetime
from typing import Set, Deque, Optional
import uvicorn

logging.basicConfig(level=logging.INFO)

# Глобальные состояния (лучше вынести в отдельный класс)
active_bybit_ws: BybitWebSocket | None = None
message_queue: Deque[str] = deque()
clients: Set[WebSocket] = set()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения (startup/shutdown)."""
    # Запускаем фоновую задачу для рассылки сообщений
    task = asyncio.create_task(broadcast_messages())
    logging.info("Сервер запущен")
    
    yield  # Здесь работает приложение
    
    # Очистка при завершении
    task.cancel()
    if active_bybit_ws:
        active_bybit_ws.exit()
    logging.info("Сервер остановлен")

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins="*",
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],  # Разрешить все методы
    allow_headers=["*"],  # Разрешить все заголовки
    expose_headers=["*"]
)

async def broadcast_messages():
    """Рассылает сообщения из очереди всем клиентам."""
    while True:
        if message_queue and clients:
            message = message_queue.popleft()
            for client in list(clients):
                try:
                    await client.send_text(message)
                except Exception as e:
                    logging.error(f"Ошибка отправки клиенту: {e}")
                    clients.discard(client)
        await asyncio.sleep(0.1)

def bybit_callback(message: dict):
    """Синхронный callback от Bybit (добавляет сообщение в очередь)."""
    try:
        # print("MSG: ", message)
        data = message["data"]
        converted_messages = list()
        for m in data:
            converted_message = dict()
            converted_message["close"] = float(m["close"])
            converted_message["high"] = float(m["high"])
            converted_message["low"] = float(m["low"])
            converted_message["open"] = float(m["open"])
            converted_message["time"] = float(m["timestamp"]) / 1000 + (3 * 60 * 60)
            converted_message["start"] = float(m["start"]) / 1000 + (3 * 60 * 60)
            converted_message["end"] = float(m["end"]) / 1000 + (3 * 60 * 60)
            converted_message["confirm"] = m["confirm"]

            converted_messages.append(converted_message)

        # print("MESSAGES: ", json.dumps(converted_messages))
        message_queue.append(json.dumps(converted_messages))
    except Exception as e:
        logging.error(f"Ошибка обработки сообщения Bybit: {e}")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    global active_bybit_ws

    try:
        # Инициализация Bybit WebSocket (если ещё не создан)
        if active_bybit_ws is None:
            active_bybit_ws = BybitWebSocket(
                testnet=False,
                channel_type="linear"
            )
            active_bybit_ws.kline_stream(
                interval=1,
                symbol="KAITOUSDT",
                callback=bybit_callback
            )

        # Ожидание сообщений от клиента
        while True:
            data = await websocket.receive_text()
            logging.info(f"Сообщение от клиента: {data}")
            # Пример обработки команды:
            if data.startswith("subscribe:"):
                symbol = data.split(":")[1]
                active_bybit_ws.orderbook_stream(depth=1, symbol=symbol)
    except WebSocketDisconnect:
        logging.info("Клиент отключился")
    finally:
        clients.discard(websocket)
        if not clients and active_bybit_ws:
            active_bybit_ws.exit()
            active_bybit_ws = None


@app.get("/kline-history")
def read_root(
    category: str,
    symbol: str,
    interval: str,
    limit: Optional[int] = None,
    end: Optional[str] = None
):
    session = HTTP(testnet=False)
    kline_params = {
        "category": category,
        "symbol": symbol,
        "interval": interval,
        "limit": limit or 200
    }
    if end:
        kline_params["end"] = (float(end) - (3 * 60 * 60)) * 1000

    klines_history = session.get_kline(**kline_params)
    klines_history = klines_history["result"]["list"]
    for candlestick_idx in range(len(klines_history)):
        kline_history_data = klines_history[candlestick_idx]
        klines_history[candlestick_idx] = {
            "time": float(kline_history_data[0]) / 1000 + (3 * 60 * 60),
            "open": float(kline_history_data[1]),
            "high": float(kline_history_data[2]),
            "low": float(kline_history_data[3]),
            "close": float(kline_history_data[4]),
        }

    return klines_history[::-1]



if __name__ == "__main__":
    uvicorn.run("main:app")