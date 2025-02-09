import asyncio
import logging
import os
from dotenv import load_dotenv

# Очередь сообщений для повторной отправки при разрыве связи
retry_queue = []

# Загрузка данных из .env
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан в переменных окружения!")

async def process_retry_queue():
    global retry_queue
    while True:
        # Копируем очередь для итерирования
        for item in retry_queue.copy():
            send_method = item.get('send_method')
            kwargs = item.get('kwargs')
            try:
                await send_method(**kwargs)
                retry_queue.remove(item)
                logging.info("Сообщение успешно отправлено из очереди.")
            except Exception as e:
                logging.error(f"Не удалось отправить сообщение из очереди: {e}")
        await asyncio.sleep(10)

async def safe_send(send_method, **kwargs):
    try:
        return await send_method(**kwargs)
    except Exception as e:
        logging.error(f"Ошибка отправки, сообщение сохранено для повторной отправки: {e}")
        retry_queue.append({
            'send_method': send_method,
            'kwargs': kwargs,
        })
        # Пытаемся уведомить пользователя о проблемах, если указан chat_id
        if 'chat_id' in kwargs:
            try:
                await bot.send_message(chat_id=kwargs['chat_id'],
                                       text="Сейчас возникли проблемы с сетью. "
                                            "Ваше сообщение будет отправлено, как только связь восстановится.")
            except Exception as inner:
                logging.error(f"Не удалось отправить уведомление пользователю: {inner}")
        return None

# В функции handle_user_message, заменяем вызовы отправки:
await safe_send(bot.send_message, chat_id=GROUP_ID, message_thread_id=topic_id, text=forward_message)

# Аналогичные изменения для других типов сообщений:
elif message.photo:
    await safe_send(bot.send_photo,
                    chat_id=GROUP_ID,
                    message_thread_id=topic_id,
                    photo=message.photo[-1].file_id,
                    caption=f"{user_tag}\n{message.caption or ''}")

# В handle_user_message, для остальных блоков (video, document, audio, voice, и прочее) аналогично:
await safe_send(bot.send_video, chat_id=GROUP_ID, message_thread_id=topic_id, video=message.video.file_id, caption=f"{user_tag}\n{message.caption or ''}")

# В функции process_admin_message, меняем отправку ответных сообщений:
if message.photo:
    await safe_send(bot.send_photo, chat_id=telegram_id, photo=message.photo[-1].file_id, caption=message.caption)

# И аналогичные изменения для остальных вызовов: send_video, send_document, send_audio, send_voice, send_message.
elif message.text:
    await safe_send(bot.send_message, chat_id=telegram_id, text=f"\n\n{message.text}")

async def main():
    global db_pool2
    try:
        db_pool2 = await get_db_pool2()  # Создание пула
        logging.info("Пул db_pool2 успешно создан")

        # Тестовый запрос
        async with db_pool2.acquire() as conn:
            await conn.execute("SELECT 1")
            logging.info("Подключение к базе данных успешно установлено")

        await log_pool_state()  # Логирование состояния пула

        # Запускаем фоновую задачу для повторной отправки сообщений
        asyncio.create_task(process_retry_queue())

        await dp.start_polling(bot)
    except Exception as e:
        logging.error(f"Ошибка при подключении к базе данных: {e}")