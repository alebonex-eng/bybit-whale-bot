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
TELEGRAM_BOT_TOKEN = "ТВІЙ_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "ТВІЙ_CHAT_ID"
MIN_TRADE_USD = 250000  # Поріг $250,000 для топ-альткоїнів
SYMBOLS = ["XRPUSDT", "SUIUSDT", "DOGEUSDT"]
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

# ==============================================================================
# НАЛАШТУВАННЯ ЛОГУВАННЯ
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

async def send_telegram_alert(session: aiohttp.ClientSession, symbol: str, side: str, price: float, volume: float, usd_val: float):
    emoji = "🟢 BUY (ПОКУПКА)" if side == "Buy" else "🔴 SELL (ПРОДАЖ)"
    
    message = (
        f"🐋 **КИТІВСЬКА УГОДА НА BYBIT**\n\n"
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
                logging.info(f"Сповіщення надіслано: {symbol} | {side} | ${usd_val:,.2f}")
    except Exception as e:
        logging.error(f"Не вдалося надіслати повідомлення у Telegram: {e}")

async def ping(ws):
    while True:
        try:
            await asyncio.sleep(20)
            await ws.send(json.dumps({"op": "ping"}))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.warning(f"Помилка відправки Ping: {e}")
            break

async def monitor_bybit_whales():
    topics = [f"publicTrade.{symbol}" for symbol in SYMBOLS]
    subscribe_msg = {"op": "subscribe", "args": topics}

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                logging.info(f"Підключення до Bybit WebSocket: {BYBIT_WS_URL}...")
                async with websockets.connect(BYBIT_WS_URL, ping_interval=None) as ws:
                    await ws.send(json.dumps(subscribe_msg))
                    logging.info(f"Успішно підписано на пари: {', '.join(SYMBOLS)}")

                    ping_task = asyncio.create_task(ping(ws))

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
                                            session, symbol, side, price, volume, usd_val
                                        )

                    finally:
                        ping_task.cancel()

            except (websockets.ConnectionClosed, websockets.WebSocketException, OSError) as e:
                logging.error(f"З'єднання втрачено: {e}. Повторна спроба через 5 секунд...")
                await asyncio.sleep(5)
            except Exception as e:
                logging.error(f"Несподівана помилка: {e}. Повторна спроба через 5 секунд...")
                await asyncio.sleep(5)

# Сервер для відповіді Render
async def handle(request):
    return web.Response(text="Bybit Whale Bot is alive!")

async def main():
    port = int(os.environ.get("PORT", 8080))
    app = web.Application()
    app.router.add_get("/", handle)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    
    logging.info(f"Веб-сервер запущено на порту {port}")
    await monitor_bybit_whales()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nБот зупинений користувачем.")
