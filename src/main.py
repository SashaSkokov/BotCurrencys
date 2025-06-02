import asyncio
import logging
import os
from datetime import datetime
from html import escape
import pandas as pd
import requests
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import FSInputFile
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from Database import create_pool, get_connection, config
from keyboards import commands, all_commands_keyboard, keyboard
from rate_limiter import RateLimiter
from src.keyboards import keyboardSubscribe


def sanitize_message(text):
    return escape(text)


API_TOKEN = config.get('bot').get('token')
API_KEY = config.get('api').get('key')
ADMIN_CHAT_ID = config.get('admin').get('chat_id')


bot = Bot(token=API_TOKEN)
dp = Dispatcher()
pool = None
STATE_SUBSCRIBE = None
STATE_ACTIVESUBSCRIBERS = None
STATE_EXPORT = None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

rate_limiter = RateLimiter(limit=100, period=300)



EXPORT_DIR = 'exports'
if not os.path.exists(EXPORT_DIR):
    os.makedirs(EXPORT_DIR)


ValueCurrencies = {'USD', 'EUR'}


def validate_currency(currency, max_length=1024):
    if currency not in ValueCurrencies or len(currency) > max_length:
        raise ValueError(f"Некорректная валюта '{currency}'")
    return currency


async def secure_delete(filename):
    try:
        with open(filename, 'w') as f:
            f.truncate()
        os.remove(filename)
    except Exception as e:
        logging.error(f"Ошибка при удалении файла: {e}")


@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer(
        'Привет! Я бот для отслеживания курсов валют.\n'
        'Команды пользователя:\n'
        '/usd — показать текущий курс доллара\n'
        '/eur — показать текущий курс евро\n'
        '/subscribe — подписаться на ежедневную рассылку курса\n'
        '/unsubscribe — отписаться от рассылки\n'
        '/mysettings — показать текущие подписки\n'
        'Для помощи отправьте команду /help',
        reply_markup=types.ReplyKeyboardRemove()
    )


async def export_subscriptions_to_excel(currency=None):
    async with get_connection() as connection:
        query = '''SELECT s.username, \
                       s.user_id, \
                       s.currencies, \
                       s.is_active, \
                       s.subscribe_date
                FROM subscriptions s \
                '''

        if currency:
            query += 'WHERE $1 = ANY(s.currencies)'
            rows = await connection.fetch(query, currency)
        else:
            rows = await connection.fetch(query)

        data = []
        for row in rows:
            try:
                record_dict = dict(row)

                if not all(key in record_dict for key in
                           ['username', 'user_id', 'currencies', 'is_active', 'subscribe_date']):
                    raise ValueError("Некорректная запись")

                record_dict['subscribe_date'] = record_dict['subscribe_date'].strftime('%Y-%m-%d %H:%M')

                data.append({
                    'Имя': record_dict['username'],
                    'ID пользователя': record_dict['user_id'],
                    'Валюта': currency,
                    'Активность': record_dict['is_active'],
                    'Дата и время подписки': record_dict['subscribe_date']
                })
            except Exception as e:
                print(f"Ошибка при обработке записи: {e}")
                continue
        if not data:
            return None

        df = pd.DataFrame(data, columns=['Имя', 'ID пользователя', 'Валюта', 'Активность', 'Дата и время подписки'])

        filename = os.path.join('subscriptions_export_{}.xlsx'.format(datetime.now().strftime('%Y%m%d_%H%M%S')))

        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Подписки', index=False)

            worksheet = writer.sheets['Подписки']

            format_left = writer.book.add_format({'align': 'left', 'valign': 'top'})
            worksheet.set_column(0, len(df.columns), None, format_left)

            for i, col in enumerate(df.columns):
                max_len = max(
                    df[col].astype(str).str.len().max(),
                    len(col)
                )
                worksheet.set_column(i, i, max_len + 2, format_left)

            header_format = writer.book.add_format({
                'bold': True,
                'align': 'center',
                'valign': 'vcenter',
                'fg_color': '#D7E4BC',
                'border': 1
            })

            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)

        return filename


@dp.message(Command("admin"))
async def start(message: types.Message):
    can_process, remaining_time = rate_limiter.can_process(message.from_user.id)

    if not can_process:
        await message.answer(f"Превышен лимит запросов!\n"
                             f"Попробуйте снова через {remaining_time}")
        return
    await message.answer(
        'Привет! Нажми кнопку ниже, чтобы зарегистрироваться:',
        reply_markup=keyboard
    )


@dp.message(Command("help"))
async def help(message: types.Message):
    can_process, remaining_time = rate_limiter.can_process(message.from_user.id)

    if not can_process:
        await message.answer(f"Превышен лимит запросов!\n"
                             f"Попробуйте снова через {remaining_time}")
        return
    if int(message.from_user.id)==int(ADMIN_CHAT_ID):
        help_text = """
Доступные команды:
/start - Начало
/usd - Курс доллара
/eur - Курс евро
/help - Помощь
/mysettings — показать текущие подписки
/subscribe - подписаться на рассылку
/unsubscribe - отписаться от рассылки
/admin - Авторизация
/export - Экспорт данных
/activeSubscribers - Посмотреть активных подписчиков
"""
    else:
        help_text = """
Доступные команды:
/start - Начало
/usd - Курс доллара
/eur - Курс евро
/help - Помощь
/mysettings — показать текущие подписки
/subscribe - подписаться на рассылку
/unsubscribe - отписаться от рассылки
"""
    sanitized_help_text = sanitize_message(help_text)
    await message.answer(sanitized_help_text)


async def get_currency_rate(currency):
    try:
        response = requests.get(
            f'https://api.exchangerate.host/convert?access_key={API_KEY}&from={currency}&to=RUB&amount=1')
        data = response.json()

        if data['success']:
            return data['result']
        else:
            return None
    except Exception as e:
        logging.error(f"Ошибка при получении курсов: {e}")
        return None


@dp.message(Command("eur"))
async def get_currency_rates(message: types.Message):
    can_process, remaining_time = rate_limiter.can_process(message.from_user.id)
    if not can_process:
        await message.answer(f"Превышен лимит запросов!\n"
                             f"Попробуйте снова через {remaining_time}")
        return
    rate = await get_currency_rate('EUR')
    if rate:
        await message.answer(f"1€ = {rate} руб.")
    else:
        await message.answer("Ошибка получения курса")


@dp.message(Command("usd"))
async def get_currency_rates(message: types.Message):
    can_process, remaining_time = rate_limiter.can_process(message.from_user.id)

    if not can_process:
        await message.answer(f"Превышен лимит запросов!\n"
                             f"Попробуйте снова через {remaining_time}")
        return
    rate = await get_currency_rate('USD')
    if rate:
        await message.answer(f"1$ = {rate} руб.")
    else:
        await message.answer("Ошибка получения курса")


@dp.message(lambda message: message.content_type == 'contact')
async def handleContact(message: types.Message):
    can_process, remaining_time = rate_limiter.can_process(message.from_user.id)

    if not can_process:
        await message.answer(f"Превышен лимит запросов!\n"
                             f"Попробуйте снова через {remaining_time}")
        return
    if message.contact:
        username = message.from_user.username or message.from_user.first_name
        phone_number = message.contact.phone_number
        user_id = message.from_user.id

        async with get_connection() as connection:
            try:

                record = await connection.fetchrow('SELECT SUBSTRING(phone_number, 2) as phone_number FROM contacts WHERE phone_number = $1', phone_number)

                if record:

                    await message.answer("Вы уже зарегистрированы в системе", reply_markup=all_commands_keyboard)
                    await bot.send_message(chat_id=ADMIN_CHAT_ID,
                                           text=f"Попытка повторной регистрации:\n"
                                                f"Номер: {phone_number}\n"
                                                f"Username: {username}")
                else:
                    await connection.execute('INSERT INTO contacts (username, phone_number, user_id) VALUES ($1, $2, $3)', username, phone_number, user_id)

                    await message.answer(f'Спасибо за регистрацию!\n'
                        f'Ваш username: {username}\n'
                        f'Ваш номер: {phone_number}', reply_markup=all_commands_keyboard)
            except Exception as e:
                logging.error(f"Ошибка при работе с БД: {e}")
                await message.answer("Произошла ошибка при сохранении данных")
    else:
        await message.answer('Ошибка при получении контакта')

@dp.message(Command("subscribe"))
@dp.message(Command("unsubscribe"))
@dp.message(Command("activeSubscribers"))
@dp.message(Command("export"))
async def handle_subscriptions(message: types.Message):
    can_process, remaining_time = rate_limiter.can_process(message.from_user.id)
    global STATE_SUBSCRIBE
    global STATE_ACTIVESUBSCRIBERS
    global STATE_EXPORT

    if not can_process:
        await message.answer(f"Превышен лимит запросов!\n"
                             f"Попробуйте снова через {remaining_time}")
        return

    if message.text == "/subscribe":
        STATE_SUBSCRIBE = True
        STATE_ACTIVESUBSCRIBERS = False
        STATE_EXPORT = False
        await message.answer("Выберите валюту для подписки:", reply_markup=keyboardSubscribe)
    elif message.text == "/unsubscribe":
        STATE_SUBSCRIBE = False
        STATE_ACTIVESUBSCRIBERS = False
        STATE_EXPORT = False
        await message.answer("Выберите валюту для отписки:", reply_markup=keyboardSubscribe)
    elif message.text == "/activeSubscribers":
        STATE_SUBSCRIBE = None
        STATE_ACTIVESUBSCRIBERS = True
        STATE_EXPORT = False
        await message.answer("Выберите валюту для проверки подписчиков:", reply_markup=keyboardSubscribe)
    elif message.text == "/export":
        STATE_SUBSCRIBE = None
        STATE_ACTIVESUBSCRIBERS = False
        STATE_EXPORT = True
        await message.answer("Выберите валюту для экспорта:", reply_markup=keyboardSubscribe)


@dp.message(lambda message: message.text in ["USD", "EUR"])
async def handleSubscription(message: types.Message):
    can_process, remaining_time = rate_limiter.can_process(message.from_user.id)
    global STATE_SUBSCRIBE
    global STATE_ACTIVESUBSCRIBERS
    global STATE_EXPORT
    user_id = message.from_user.id
    currency = message.text
    username = message.from_user.username or message.from_user.first_name
    adminIsContact = ""

    if not can_process:
        await message.answer(f"Превышен лимит запросов!\n"
                             f"Попробуйте снова через {remaining_time}")
        return

    async with get_connection() as connection:
        adminIsContact = await connection.fetchval('SELECT user_id FROM contacts WHERE user_id = $1', user_id)

    if STATE_SUBSCRIBE:

        date_now = datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M')
        async with get_connection() as connection:
            try:
                subscription = await connection.fetchval('SELECT user_id FROM subscriptions WHERE user_id = $1',user_id)

                if subscription:
                    currencys = await connection.fetchval('SELECT CASE WHEN NOT $1 = ANY(currencies) THEN TRUE ELSE FALSE END FROM subscriptions WHERE user_id = $2', currency, user_id)

                    if currencys:

                        await connection.execute('UPDATE subscriptions SET currencies = array_append(currencies, $1), is_active = TRUE, subscribe_date = $2 WHERE user_id = $3',currency, date_now, user_id)
                        await message.answer(f"Поздравляем🚀 \nВы подписались на рассылку курса {currency}")
                    else:
                        await message.answer(f"Вы уже подписаны на рассылку курса {currency}")
                else:

                    await connection.execute(
                        'INSERT INTO subscriptions (user_id, currencies, username, subscribe_date) VALUES ($1, ARRAY[$2], $3, $4)', user_id, currency, username, date_now)
                    await message.answer(f"Поздравляем🚀 \nВы подписались на рассылку курса {currency}")
            except Exception as e:
                logging.error(f"Ошибка при подписке: {e}")
                await message.answer("Произошла ошибка при подписке")
    elif STATE_SUBSCRIBE is False:

        async with get_connection() as connection:
            try:
                currency = message.text
                result = await connection.fetchval(
                    "SELECT $1 = ANY(currencies) FROM subscriptions WHERE user_id = $2",
                    currency, user_id
                )
                if result:
                    await connection.execute(
                        "UPDATE subscriptions SET currencies = array_remove(currencies, $1) WHERE user_id = $2",
                        currency, user_id)

                    result = await connection.fetchval(
                        "SELECT currencies IS NOT NULL AND currencies != '{}' FROM subscriptions WHERE user_id = $1",
                        user_id)
                    if result:
                        await message.answer("Вы успешно отписались от рассылки")

                    else:
                        await connection.execute(
                            "UPDATE subscriptions SET is_active = FALSE WHERE user_id = $1",
                            user_id)

                        await message.answer("Вы успешно отписались от рассылки")
                else:
                    await message.answer("Вы не подписаны на данную рассылку")
            except Exception as e:
                logging.error(f"Ошибка при отписке: {e}")
                await message.answer("Произошла ошибка при отписке")

    if STATE_ACTIVESUBSCRIBERS:
        if int(message.from_user.id) == int(ADMIN_CHAT_ID):
            if adminIsContact is None:
                await message.answer("Зарегистрируйтесь, прежде чем воспользоваться командой")
                return
            try:
                currency = message.text
                if not currency:
                    raise ValueError("Некорректный код валюты")

                async with get_connection() as connection:
                    rows = await connection.fetch('''SELECT s.username, s.user_id, s.currencies, s.is_active
                                                     FROM subscriptions s
                                                     WHERE $1 = ANY (s.currencies)
                                                  ''', currency)

                    if not rows:
                        await message.answer(f"Нет пользователей с подпиской на валюту {currency}")
                        return

                    response = f"Пользователи с подпиской на {currency}:\n\n"

                    for row in rows:
                        response += f"ID: {row['user_id']}\n"
                        response += f"Имя: {row['username']}\n"
                        response += f"Все подписки: {', '.join(row['currencies'])}\n"
                        response += f"Статус: {'Активна' if row['is_active'] else 'Неактивна'}\n\n"


                    await message.answer(response)


            except ValueError as ve:
                await message.answer(f"Ошибка: {ve}")
            except Exception as e:
                logging.error(f"Ошибка при получении подписок: {e}")
                await message.answer("Произошла ошибка при получении данных")
        else:
            await message.answer("У вас нет прав для использования этой команды")

    if STATE_EXPORT:
        if int(message.from_user.id) == int(ADMIN_CHAT_ID):
            if adminIsContact is None:
                await message.answer("Зарегистрируйтесь, прежде чем воспользоваться командой")
                return
            while True:
                try:
                    currency = message.text

                    filename = await export_subscriptions_to_excel(currency)
                    if filename is None:
                        await message.answer(f"Нет подписок по валюте {currency}")
                        return
                    if os.path.exists(filename):
                        document = FSInputFile(filename)
                        await bot.send_document(
                            chat_id=message.chat.id,
                            document=document,
                            caption=f"Экспорт подписок по валюте {currency}"
                        )
                        await secure_delete(filename)
                    else:
                        await message.answer("Ошибка при создании файла экспорта")

                    break

                except ValueError as ve:
                    await message.answer("Пожалуйста, введите команду в формате: /export USD")
                    return

                except Exception as e:
                    logging.error(f"Ошибка при экспорте: {e}")
                    await message.answer("Произошла ошибка при экспорте")
                    return

        else:
            await message.answer("У вас нет доступа к этой команде")



@dp.message(lambda message: message.text in ["Все команды"])
async def handleSubscription(message: types.Message):
    can_process, remaining_time = rate_limiter.can_process(message.from_user.id)

    if not can_process:
        await message.answer(f"Превышен лимит запросов!\n"
                             f"Попробуйте снова через {remaining_time}")
        return
    await help(message)
    await message.answer("Вы перешли во вкладку 'Все команды'", reply_markup=types.ReplyKeyboardRemove())


async def is_bot_blocked(user_id):
    try:
        await bot.send_message(user_id, "Проверка доступности", disable_notification=True)
        return False
    except Exception as e:
        if "bot was blocked by the user" in str(e):
            return True
        return False


async def sendSubscriptions():
    async with get_connection() as connection:
        try:
            subscriptions = await connection.fetch('SELECT user_id, currencies FROM subscriptions WHERE is_active = TRUE')

            for subscription in subscriptions:
                rate = await get_currency_rate(str(subscription['currencies'][0]))

                if rate:
                    if await is_bot_blocked(subscription['user_id']):
                        await connection.execute('DELETE FROM subscriptions WHERE user_id = $1', subscription['user_id'])
                        continue
                    try:
                        await bot.send_message(chat_id=subscription['user_id'],
                                               text=f"Курс {subscription['currencies'][0]} на сегодня:\n"
                                                    f"1 {subscription['currencies'][0]} = {rate:.2f} RUB")

                    except Exception as e:
                        logging.error(f"Ошибка при отправке сообщения: {e}")
                else:
                    await bot.send_message(chat_id=subscription['user_id'], text="Не удалось получить курс валюты")
        except Exception as e:
            logging.error(f"Ошибка при отправке рассылки: {e}")


scheduler = AsyncIOScheduler()
scheduler.add_job(sendSubscriptions, 'cron', hour=10, minute=00, timezone='Europe/Moscow')


@dp.message(Command("mysettings"))
async def checkSubscription(message: types.Message):
    can_process, remaining_time = rate_limiter.can_process(message.from_user.id)

    if not can_process:
        await message.answer(f"Превышен лимит запросов!\n"
                             f"Попробуйте снова через {remaining_time}")
        return

    user_id = message.from_user.id

    async with get_connection() as connection:
        subscriptionIsNull = await connection.fetchrow("SELECT is_active FROM subscriptions WHERE user_id = $1", user_id)

        if subscriptionIsNull is not None:
            if subscriptionIsNull['is_active']:
                subscription = await connection.fetchrow('SELECT * FROM subscriptions WHERE user_id = $1', user_id)
                currencies = subscription['currencies']

                is_active = subscription['is_active']

                response = f"Ваши подписки:\n"
                response += f"Подписки: {', '.join(currencies)}\n"
                response += f"Статус: {'Активна' if is_active else 'Неактивна'}"

                await message.answer(response)
            else:
                await message.answer("У вас нет активной подписки")
        else:
            await message.answer("У вас нет активной подписки")

async def main():
    global STATE_SUBSCRIBE
    global STATE_ACTIVESUBSCRIBERS
    global STATE_EXPORT

    await bot.set_my_commands(commands)
    try:
        await create_pool()
        scheduler.start()

        try:
            await dp.start_polling(bot, skip_updates=True)
        except asyncio.CancelledError:
            logging.warning("Бот остановлен пользователем")
        except KeyboardInterrupt:
            logging.info("Бот остановлен пользователем")
        except Exception as e:
            logging.error(f"Ошибка при запуске polling: {e}")

    finally:
        if pool:
            await pool.close()
            scheduler.shutdown()


if __name__ == '__main__':
    asyncio.run(main())