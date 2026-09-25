import os
import asyncio
import json
import logging
import websockets
import aiohttp
from aiohttp import web

# ==============================================================================
# НАЛАШТУВАННЯ БОТА
# ==============================================================================
TELEGRAM_BOT_TOKEN = "8919783780:AAG3ScU7jUwFAyaF7hWfHINMCQrxqyYq2g0"
TELEGRAM_CHAT_ID = "498333100"
MIN_TRADE_USD = 250000  # Поріг $250,000 для топ-альткоїнів
SYMBOLS = ["XRPUSDT",  "SUIUSDT", "DOGEUSDT"]

# URL-адреси WebSocket для обох бірж
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
BINANCE_WS_URL = "wss://fstream.binance.com/stream?streams=" + "/".join([f"{s.lower()}@aggTrade" for s in SYMBOLS])

# ==============================================================================
# НАЛАШТУВАННЯ ЛОГУВАННЯ
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Універсальна функція відправки сповіщень у Telegram
async def send_telegram_alert(session: aiohttp.ClientSession, exchange: str, symbol: str, side: str, price: float, volume: float, usd_val: float):
    emoji = "🟢 BUY (ПОКУПКА)" if side == "Buy" else "🔴 SELL (ПРОДАЖ)"
    
    message = (
        f"🐋 **КИТІВСЬКА УГОДА НА {exchange.upper()}**\n\n"
        f"🏦 **Біржа:** {exchange}\n"
        f"📌 **Торгова пара:** #{symbol}\n"
        f"📊 **Напрямок:** {emoji}\n"
        f"💵 **Ціна:** `{price:,.4f} USDT`\n"
        f"📦 **Об'єм:** `{volume:,.2f}`\n"
        f"💰 **Сума угоди:** `${usd_val:,.2f}`"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
    }

    try:
        async with session.post(url, json=payload, timeout=10) as resp:
            if resp.status != 200:
                text = await resp.text()
                logging.error(f"Помилка Telegram API [{resp.status}]: {text}")
            else:
                logging.info(f"[{exchange}] Сповіщення надіслано: {symbol} | {side} | ${usd_val:,.2f}")
    except Exception as e:
        logging.error(f"Не вдалося надіслати повідомлення у Telegram: {e}")

# --- МОНІТОРИНГ BYBIT ---
async def ping_bybit(ws):
    while True:
        try:
            await asyncio.sleep(20)
            await ws.send(json.dumps({"op": "ping"}))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.warning(f"Помилка відправки Ping на Bybit: {e}")
            break

async def monitor_bybit_whales():
    topics = [f"publicTrade.{symbol}" for symbol in SYMBOLS]
    subscribe_msg = {"op": "subscribe", "args": topics}

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                logging.info(f"Підключення до Bybit WebSocket...")
                async with websockets.connect(BYBIT_WS_URL, ping_interval=None) as ws:
                    await ws.send(json.dumps(subscribe_msg))
                    logging.info(f"Успішно підписано на Bybit пари: {', '.join(SYMBOLS)}")

                    ping_task = asyncio.create_task(ping_bybit(ws))

                    try:
                        while True:
                            response = await ws.recv()
                            data = json.loads(response)

                            if "topic" in data and "data" in data:
                                for trade in data["data"]:
                                    price = float(trade["p"])
                                    volume = float(trade["v"])
                                    side = trade["S"]
                                    symbol = trade["s"]
                                    usd_val = price * volume

                                    if usd_val >= MIN_TRADE_USD:
                                        await send_telegram_alert(
                                            session, "Bybit", symbol, side, price, volume, usd_val
                                        )

                    finally:
                        ping_task.cancel()

            except (websockets.ConnectionClosed, websockets.WebSocketException, OSError) as e:
                logging.error(f"Bybit: з'єднання втрачено: {e}. Повторна спроба через 5 сек...")
                await asyncio.sleep(5)
            except Exception as e:
                logging.error(f"Bybit: несподівана помилка: {e}. Повторна спроба через 5 сек...")
                await asyncio.sleep(5)

# --- МОНІТОРИНГ BINANCE ---
async def monitor_binance_whales():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                logging.info(f"Підключення до Binance WebSocket...")
                async with websockets.connect(BINANCE_WS_URL, ping_interval=20) as ws:
                    logging.info(f"Успішно підписано на Binance пари: {', '.join(SYMBOLS)}")

                    while True:
                        response = await ws.recv()
                        msg = json.loads(response)

                        if "data" in msg:
                            trade = msg["data"]
                            symbol = trade["s"]
                            price = float(trade["p"])
                            volume = float(trade["q"])
                            usd_val = price * volume

                            # Для Binance: m = True означає, що покупцем був мейкер (тобто агресивна продажа)
                            side = "Sell" if trade["m"] else "Buy"

                            if usd_val >= MIN_TRADE_USD:
                                await send_telegram_alert(
                                    session, "Binance", symbol, side, price, volume, usd_val
                                )

            except (websockets.ConnectionClosed, websockets.WebSocketException, OSError) as e:
                logging.error(f"Binance: з'єднання втрачено: {e}. Повторна спроба через 5 сек...")
                await asyncio.sleep(5)
            except Exception as e:
                logging.error(f"Binance: несподівана помилка: {e}. Повторна спроба через 5 сек...")
                await asyncio.sleep(5)

# --- ВЕБ-СЕРВЕР ДЛЯ RENDER ---
async def handle(request):
    return web.Response(text="Crypto Whale Bot (Bybit + Binance) is alive!")

async def main():
    port = int(os.environ.get("PORT", 8080))
    app = web.Application()
    app.router.add_get("/", handle)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    
    logging.info(f"Веб-сервер запущено на порту {port}")
    
    # Запускаємо моніторинг двох бірж паралельно
    await asyncio.gather(
        monitor_bybit_whales(),
        monitor_binance_whales()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nБот зупинений користувачем.")
