import asyncio
import logging
from pathlib import Path
import io
from typing import Optional
import aiohttp

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    KeyboardButton,
    ReplyKeyboardMarkup,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BufferedInputFile,
)
from instagrapi import Client
from instagrapi.exceptions import (
    ChallengeRequired, BadPassword, UnknownError
)
from requests.exceptions import ProxyError, ConnectionError, ReadTimeout, RequestException
from dotenv import load_dotenv
import os

from db import init_db, add_account, get_folders, add_folder, get_accounts, get_account_by_id

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()
API_TOKEN = os.getenv("BOT_TOKEN")
if not API_TOKEN:
    raise ValueError("Необходимо установить BOT_TOKEN в .env файле")

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Создание папки для сессий, если ее нет
Path("sessions").mkdir(exist_ok=True)


# Определяем состояния
class AppStates(StatesGroup):
    # Добавление
    choosing_folder_for_single = State()
    choosing_folder_for_bulk = State()
    creating_new_folder_for_single = State()
    creating_new_folder_for_bulk = State()
    waiting_for_single_details = State()
    waiting_for_single_proxy = State()
    waiting_for_account_file = State()
    waiting_for_proxy_file = State()
    
    # Удаление
    choosing_account_to_delete = State()
    confirming_delete = State()

# --- Клавиатуры ---
keyboard_main = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Аккаунты"), KeyboardButton(text="Папки")],
        [KeyboardButton(text="Рассылки"), KeyboardButton(text="Прогрев")],
    ],
    resize_keyboard=True,
)

keyboard_accounts = ReplyKeyboardMarkup(
        keyboard=[
        [KeyboardButton(text="Одиночное добавление"), KeyboardButton(text="Массовое добавление")],
        [KeyboardButton(text="Список аккаунтов"), KeyboardButton(text="Удалить аккаунт")],
        [KeyboardButton(text="⬅️ Назад в главное меню")],
        ],
        resize_keyboard=True,
    )

# --- Обработчики главного меню ---
@dp.message(CommandStart())
async def handle_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Добро пожаловать!", reply_markup=keyboard_main)

@dp.message(F.text == "⬅️ Назад в главное меню")
async def handle_back_to_main(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню:", reply_markup=keyboard_main)

# --- Раздел "Аккаунты" ---
@dp.message(F.text == "Аккаунты")
async def handle_accounts(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Выберите действие:", reply_markup=keyboard_accounts)

# --- Универсальные функции ---
async def show_folder_selection(message: Message, state: FSMContext, next_state: State):
    folders = await get_folders()
    buttons = [[KeyboardButton(text=f"📁 {f['name']}")] for f in folders]
    buttons.append([KeyboardButton(text="➕ Создать новую папку")])
    buttons.append([KeyboardButton(text="⬅️ Назад в меню аккаунтов")])
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    await state.update_data(next_state=next_state.state)
    await message.answer("Выберите папку или создайте новую:", reply_markup=keyboard)

async def process_new_folder_creation(message: Message, state: FSMContext, next_state_on_success: State):
    folder_name = message.text
    if not folder_name or folder_name == "Отмена":
        await message.answer("Создание папки отменено.")
        # Возвращаемся к выбору папки
        current_state = await state.get_state()
        if current_state == AppStates.creating_new_folder_for_single.state:
            await state.set_state(AppStates.choosing_folder_for_single)
            await show_folder_selection(message, state, AppStates.waiting_for_single_details)
        elif current_state == AppStates.creating_new_folder_for_bulk.state:
            await state.set_state(AppStates.choosing_folder_for_bulk)
            await show_folder_selection(message, state, AppStates.waiting_for_account_file)
        return
        
    folder_id = await add_folder(folder_name)
    if folder_id:
        await state.update_data(folder_id=folder_id)
        await state.set_state(next_state_on_success)
        if next_state_on_success == AppStates.waiting_for_single_details:
            await message.answer(f"Папка '{folder_name}' создана. Введите данные аккаунта:\n`login:password:email:email_password`", parse_mode="Markdown")
        elif next_state_on_success == AppStates.waiting_for_account_file:
            await message.answer(f"Папка '{folder_name}' создана. Пришлите TXT файл с аккаунтами.")
    else:
        await message.answer(f"Папка '{folder_name}' уже существует. Попробуйте снова.")


# --- Одиночное добавление ---
@dp.message(F.text == "Одиночное добавление")
async def handle_single_add_start(message: Message, state: FSMContext):
    await state.set_state(AppStates.choosing_folder_for_single)
    await show_folder_selection(message, state, AppStates.waiting_for_single_details)

@dp.message(AppStates.choosing_folder_for_single)
async def handle_folder_choice_for_single(message: Message, state: FSMContext):
    text = message.text
    if text == "➕ Создать новую папку":
        await state.set_state(AppStates.creating_new_folder_for_single)
        await message.answer("Введите название для новой папки:", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True))
    elif text and text.startswith("📁 "):
        folder_name = text.replace("📁 ", "")
        folders = await get_folders()
        folder = next((f for f in folders if f['name'] == folder_name), None)
        if folder:
            await state.update_data(folder_id=folder['id'])
            await state.set_state(AppStates.waiting_for_single_details)
            await message.answer(f"Выбрана папка '{folder_name}'. Введите данные:\n`login:password:email:email_password`", parse_mode="Markdown")
        else:
            await message.answer("Папка не найдена. Попробуйте снова.")
    elif text == "⬅️ Назад в меню аккаунтов":
        await handle_accounts(message, state)

@dp.message(AppStates.creating_new_folder_for_single)
async def handle_new_folder_for_single(message: Message, state: FSMContext):
    await process_new_folder_creation(message, state, AppStates.waiting_for_single_details)

@dp.message(AppStates.waiting_for_single_details)
async def handle_single_details(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("Пожалуйста, отправьте текстовое сообщение.")
        return
    try:
        login, password, email, email_password = message.text.split(':')
        await state.update_data(login=login, password=password, email=email, email_password=email_password)
        await state.set_state(AppStates.waiting_for_single_proxy)
        await message.answer("Введите прокси: `http(s)/socks5://user:pass@ip:port`", parse_mode="Markdown")
    except (ValueError, TypeError):
        await message.answer("Неверный формат. Попробуйте еще раз: `login:password:email:email_password`", parse_mode="Markdown")

@dp.message(AppStates.waiting_for_single_proxy)
async def handle_single_proxy(message: Message, state: FSMContext):
    proxy = message.text
    
    # Проверяем, хочет ли пользователь добавить без прокси
    use_proxy = proxy and proxy.lower() not in ['нет', 'no', 'none']
    
    if use_proxy and isinstance(proxy, str) and not (proxy.startswith('http') or proxy.startswith('socks5')):
        await message.answer("Неверный формат прокси. Попробуйте снова или напишите 'нет'.")
        return
    elif use_proxy and not isinstance(proxy, str):
        await message.answer("Пожалуйста, отправьте текст: прокси или слово 'нет'.")
        return

    user_data = await state.get_data()
    account_data = {
        **user_data, 
        "username": user_data['login'], 
        "proxy": proxy if use_proxy else None
    }
    
    await message.answer(f"Начинаю вход для {account_data['username']}...")
    success, msg = await attempt_login_and_save(account_data)
    
    if success:
        await message.answer(f"✅ Успешно! Аккаунт '{msg}' добавлен.")
    else:
        await message.answer(f"❌ Ошибка: {msg}")
    
    await state.clear()
    await handle_accounts(message, state)

# --- Массовое добавление ---
@dp.message(F.text == "Массовое добавление")
async def handle_bulk_add_start(message: Message, state: FSMContext):
    await state.set_state(AppStates.choosing_folder_for_bulk)
    await show_folder_selection(message, state, AppStates.waiting_for_account_file)

@dp.message(AppStates.choosing_folder_for_bulk)
async def handle_folder_choice_for_bulk(message: Message, state: FSMContext):
    text = message.text
    if text == "➕ Создать новую папку":
        await state.set_state(AppStates.creating_new_folder_for_bulk)
        await message.answer("Введите название для новой папки:", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True))
    elif text and text.startswith("📁 "):
        folder_name = text.replace("📁 ", "")
        folders = await get_folders()
        folder = next((f for f in folders if f['name'] == folder_name), None)
        if folder:
            await state.update_data(folder_id=folder['id'])
            await state.set_state(AppStates.waiting_for_account_file)
            await message.answer(f"Выбрана папка '{folder_name}'. Пришлите TXT файл с аккаунтами.")
        else:
            await message.answer("Папка не найдена. Попробуйте снова.")
    elif text == "⬅️ Назад в меню аккаунтов":
        await handle_accounts(message, state)

@dp.message(AppStates.creating_new_folder_for_bulk)
async def handle_new_folder_for_bulk(message: Message, state: FSMContext):
    await process_new_folder_creation(message, state, AppStates.waiting_for_account_file)

@dp.message(AppStates.waiting_for_account_file, F.document)
async def handle_account_file(message: Message, state: FSMContext):
    if not message.document:
        return
    try:
        file = await bot.get_file(message.document.file_id)
        if not file.file_path:
            raise ValueError("File path is not available")
        
        file_bytes = await bot.download_file(file.file_path)
        if not file_bytes:
            raise ValueError("Could not download file")

        lines = file_bytes.read().decode('utf-8').splitlines()
        lines = [line.strip() for line in lines if line.strip()]
        if not lines:
            await message.answer("Файл с аккаунтами пуст.")
            return
                
        _ = lines[0].split(':') # Проверка формата
        await state.update_data(account_lines=lines)
        await state.set_state(AppStates.waiting_for_proxy_file)
        await message.answer(f"Получено {len(lines)} аккаунтов. Теперь пришлите TXT файл с прокси.")
    except Exception as e:
        logger.error(f"Account file error: {e}")
        await message.answer("Ошибка при обработке файла. Убедитесь, что формат `login:pass:mail:mail_pass` верный.")

@dp.message(AppStates.waiting_for_proxy_file, F.document)
async def handle_proxy_file(message: Message, state: FSMContext):
    if not message.document:
        return
    try:
        file = await bot.get_file(message.document.file_id)
        if not file.file_path:
            raise ValueError("File path is not available")

        file_bytes = await bot.download_file(file.file_path)
        if not file_bytes:
            raise ValueError("Could not download file")
        
        proxy_lines = file_bytes.read().decode('utf-8').splitlines()
        proxy_lines = [line.strip() for line in proxy_lines if line.strip()]
        
        # Получаем аккаунты ДО проверки, чтобы знать, сколько прокси нужно
        user_data = await state.get_data()
        account_lines = user_data.get('account_lines', [])
        folder_id = user_data.get('folder_id')

        # Если прокси больше чем аккаунтов, обрезаем список прокси
        if len(proxy_lines) > len(account_lines):
            logger.info(f"Прокси ({len(proxy_lines)}) больше, чем аккаунтов ({len(account_lines)}). Будет проверено только {len(account_lines)} прокси.")
            proxy_lines = proxy_lines[:len(account_lines)]

        # --- Проверка прокси в параллель ---
        await message.answer(
            f"Начинаю проверку {len(proxy_lines)} прокси (по количеству аккаунтов)..."
        )
        total_proxies = len(proxy_lines)
        logger.info(f"Начинаю проверку {total_proxies} прокси параллельно...")

        timeout_seconds = 30
        tasks = [
            asyncio.wait_for(check_proxy(proxy), timeout_seconds)
            for proxy in proxy_lines
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        working_proxies = []
        for proxy, res in zip(proxy_lines, results):
            if isinstance(res, Exception):
                logger.warning(
                    f"Прокси {proxy[:40]}... НЕ РАБОТАЕТ. Причина: {str(res)}"
                )
                continue

            is_ok, result = res
            if is_ok:
                working_proxies.append(result)
                logger.info(f"Прокси {result[:40]}... OK")
            else:
                logger.warning(
                    f"Прокси {proxy[:40]}... НЕ РАБОТАЕТ. Причина: {result}"
                )

        logger.info(
            f"Проверка прокси завершена. Работает: {len(working_proxies)}/{total_proxies}"
        )
        await message.answer(
            f"Проверка завершена. Работающих прокси: {len(working_proxies)}/{total_proxies}."
        )
        # --- Конец проверки ---

        if not working_proxies:
            await message.answer("Нет работающих прокси. Операция отменена.")
            await state.clear()
            await handle_accounts(message, state)
            return

        if len(account_lines) > len(working_proxies):
            await message.answer(f"Ошибка: Аккаунтов ({len(account_lines)}) больше, чем РАБОЧИХ прокси ({len(working_proxies)}).")
            await state.clear()
            await handle_accounts(message, state)
            return

        await message.answer(f"Начинаю добавление {len(account_lines)} аккаунтов, используя рабочие прокси...")
        report_lines = []
        total_accounts = len(account_lines)
        last_status = ""
        for i, acc_line in enumerate(account_lines):
            try:
                login, password, email, email_password = acc_line.split(':')
                
                status_line = f"({i + 1}/{total_accounts}) Вход: {login}... {last_status}"
                print(status_line.ljust(120), end='\r', flush=True)

                account_data = {
                    "username": login, "password": password, "email": email,
                    "email_password": email_password, "proxy": working_proxies[i],
                    "folder_id": folder_id
                }
                success, msg = await attempt_login_and_save(account_data)
                
                if success:
                    last_status = ""
                else:
                    last_status = f"| Предыдущий ({login}): Неудача"
                
                report_lines.append(f"[{'УСПЕХ' if success else 'ОШИБКА'}] {login}: {msg}")
            except Exception as e:
                last_status = f"| Предыдущий ({acc_line.split(':')[0]}): Крит. ошибка"
                report_lines.append(f"[КРИТ. ОШИБКА] Строка '{acc_line[:15]}...': {e}")
        
        print("".ljust(120), end="\r", flush=True)
        logger.info("Массовое добавление завершено.")

        report_file = BufferedInputFile("\n".join(report_lines).encode('utf-8'), "login_report.txt")
        await message.answer_document(report_file, caption="Отчет о массовом добавлении.")
            
    except Exception as e:
        logger.error(f"Ошибка обработки файлов: {e}")
        await message.answer("Ошибка при обработке файлов.")
    finally:
        await state.clear()
        await handle_accounts(message, state)


# --- Функция проверки прокси ---
async def check_proxy(proxy: str) -> tuple[bool, str]:
    """Проверяет прокси и возвращает кортеж (is_ok, result).
    Если is_ok=True, result содержит нормализованный прокси с указанием протокола.
    Если is_ok=False, result содержит текст ошибки."""
    if not proxy:
        return False, "нет"

    # Определяем, какой URI проверять, socks5 или http
    uri_to_check = proxy
    normalized_proxy = proxy
    if "socks5" not in proxy and "http" not in proxy:
        # Если протокол не указан, сначала пробуем http
        normalized_proxy = f"http://{proxy}"
        uri_to_check = normalized_proxy
    
    try:
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get("https://httpbin.org/ip", proxy=uri_to_check) as response:
                if response.status == 200:
                    return True, normalized_proxy
                else:
                    return False, f"Неожиданный статус-код: {response.status}"

    except (aiohttp.ClientHttpProxyError, aiohttp.ClientProxyConnectionError) as e:
        return False, str(e)
    except aiohttp.ClientConnectorError as e:
        # Если http не сработал, а в исходной строке не было протокола, пробуем socks5
        if "://" not in proxy:
            try:
                normalized_proxy = f"socks5://{proxy}"
                timeout = aiohttp.ClientTimeout(total=60)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get("https://httpbin.org/ip", proxy=normalized_proxy) as response:
                        if response.status == 200:
                            return True, normalized_proxy
                        else:
                            return False, f"SOCKS5: Неожиданный статус-код: {response.status}"
            except Exception as socks_e:
                return False, str(socks_e)

        return False, str(e)
    except asyncio.TimeoutError:
        return False, "Таймаут соединения"
    except Exception as e:
        return False, str(e)

# --- Функция логина ---
async def attempt_login_and_save(details: dict) -> tuple[bool, str]:
    cl = Client()
    cl.request_timeout = 90
    username = details.get("username", "")
    session_file = Path("sessions") / f"{username}.json"
    
    # --- Определение типа прокси для instagrapi ---
    proxy = details.get("proxy")
    if proxy and "socks5" in proxy:
        # instagrapi использует немного другой формат для socks5
        if not proxy.startswith("socks5h://"):
            proxy = proxy.replace("socks5://", "socks5h://")
    # ---

    try:
        if proxy:
            cl.set_proxy(proxy)
        
        # Запускаем блокирующие операции в отдельном потоке, чтобы не морозить бота
        await asyncio.to_thread(cl.login, username, details.get("password", ""))
        await asyncio.to_thread(cl.dump_settings, session_file)
        
        added = await add_account(
            username=username,
            password=details.get("password", ""),
            email=details.get("email", ""),
            email_password=details.get("email_password", ""),
            proxy=details.get("proxy"), # Сохраняем оригинальный прокси
            folder_id=details.get("folder_id")
        )
        return (True, username) if added else (False, "Уже существует в БД")
    except ChallengeRequired:
        logger.warning(f"Login failed for {username}: Challenge Required")
        return False, "Требуется верификация (checkpoint)"
    except BadPassword:
        logger.warning(f"Login failed for {username}: Bad Password")
        return False, "Неверный пароль"
    except (ProxyError, ConnectionError, ReadTimeout, RequestException) as e:
        # Объединяем все сетевые ошибки, включая ReadTimeout / ConnectTimeout
        logger.error(f"Login failed for {username} with network error: {e}")
        return False, "Ошибка прокси или таймаут соединения"
    except UnknownError as e:
        logger.error(f"Login failed for {username} with unknown error: {e}")
        return False, f"Неизвестная ошибка Instagram: {e}"
    except Exception as e:
        logger.error(f"Login failed for {username} with generic error: {e}")
        return False, str(e)

# --- Список и удаление ---
@dp.message(F.text == "Список аккаунтов")
async def handle_list_accounts(message: Message):
    accounts = await get_accounts()
    if not accounts:
        await message.answer("Аккаунтов нет.")
        return
    
    acc_list = "\n".join([f"`{acc['id']}`: {acc['username']} (Папка: {acc.get('folder_id', 'N/A')})" for acc in accounts])
    await message.answer(f"Список аккаунтов:\n{acc_list}", parse_mode="Markdown")

@dp.message(F.text == "Удалить аккаунт")
async def handle_delete_start(message: Message, state: FSMContext):
    accounts = await get_accounts()
    if not accounts:
        await message.answer("Нечего удалять.")
        return
    
    buttons = [
        [KeyboardButton(text=f"{acc['id']}: {acc['username']}")] for acc in accounts
    ]
    buttons.append([KeyboardButton(text="Отмена")])
    keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    
    await state.set_state(AppStates.choosing_account_to_delete)
    await message.answer("Выберите аккаунт для удаления:", reply_markup=keyboard)

@dp.message(AppStates.choosing_account_to_delete)
async def handle_delete_choice(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("Пожалуйста, используйте кнопки для выбора.")
        return
    if message.text == "Отмена":
        await state.clear()
        await handle_accounts(message, state)
        return
    
    try:
        acc_id = int(message.text.split(':')[0])
        # Здесь должна быть логика удаления из db.py, которую нужно добавить
        # await delete_account(acc_id) 
        await message.answer(f"Аккаунт ID {acc_id} будет удален (логика в разработке).")
        await state.clear()
        await handle_accounts(message, state)
    except (ValueError, IndexError):
        await message.answer("Неверный выбор. Нажмите на кнопку.")


# --- Main ---
async def main():
    await init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
