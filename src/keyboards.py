from aiogram.types import BotCommand, ReplyKeyboardMarkup, KeyboardButton

commands = [
    BotCommand(command='start', description='Начало'),
    BotCommand(command='usd', description='Курс доллара'),
    BotCommand(command='eur', description='Курс евро'),
    BotCommand(command='subscribe', description="Подписаться на ежедневную рассылку курса"),
    BotCommand(command='unsubscribe', description="Отписаться от рассылки"),
    BotCommand(command='mysettings', description="Показать текущие подписки"),
    BotCommand(command='help', description="Помощь")
]

all_commands_keyboard = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Все команды")]],
    resize_keyboard=True
)

keyboardUnsubscribe = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="USD")],
        [KeyboardButton(text="EUR")],
        [KeyboardButton(text="Отменить все подписки")]
    ],
    resize_keyboard=True
)

keyboardSubscribe = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="USD")],
            [KeyboardButton(text="EUR")],
            [KeyboardButton(text="Все команды")]], resize_keyboard=True)

keyboard = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Поделиться контактом", request_contact=True)]],
    resize_keyboard=True
)