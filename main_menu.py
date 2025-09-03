#!/usr/bin/env python3
from __future__ import annotations
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import importlib
from typing import Callable, Dict

WELCOME = (
    "Приветствую Вас в универсальной программе для обработки файлов Kibana!\n"
    "\n Перед началом работы просьба ознакомиться с Workflow программы:\n"
    '\n* "Шаг 1" - принимает единый файл с traceID, формирует файлы-группы по 3500 объектов и формирует DSL конструкции для каждой отдельной группы;'
    '\n* "Шаг 2" - принимает несколько файлов CSV, вычленяет только данные по заданному Вами объекту (например plasticID), формирует список указанных объектов, удаляет дубли объектов, сохраняет в виде единого файла txt;'
    '\n* "Шаг 3" - Принимает txt файл (из шага 2), запрашивает имя таблицы БД, запрашивает конкретное поле для фильтрации с предикатом IN, формирует файлы-группы для SQL-запросов с учётом ограничения в 10000 символов каждый, формирует в каждом файле-группе SQL-запрос, передавая набор указанных значений.'
    '\n* "Шаг 4" - Принимает файлы EXEL из EQ и соединяет в один файл EXEL для последующего склеивания таблиц.'
    '\n* "Шаг 5" - Принимает два EXEL файла (файл с клиентскими данными CUS, cardId, Account - из шага 2 и файл, полученный в результате объединения выгрузок из БД EQ), затем склеивает две таблицы посредством общего ключа и выгружает готовый общий файл.'
    '\n'
    '\n Все шаги рекомендуется выполнять последовательно\n'
)

PROMPT = "Для продолжения работы введите номер шага.\nИли нажмите 0 для выхода из программы\n> "

def run_traceid_processor() -> None:
    mod = importlib.import_module("traceid_processor")
    if hasattr(mod, "main") and callable(mod.main):
        mod.main()
    else:
        print("Ошибка: в модуле traceid_processor не найдена функция main().")

def run_exel_processor() -> None:
    mod = importlib.import_module("exel_processor")
    if hasattr(mod, "main") and callable(mod.main):
        mod.main()
    else:
        print("Ошибка: в модуле exel_processor не найдена функция main().")

def run_sql_generator() -> None:
    mod = importlib.import_module("sql_generator")
    if hasattr(mod, "main") and callable(mod.main):
        mod.main()
    else:
        print("Ошибка: в модуле sql_generator не найдена функция main().")

def run_exel_processor_EQ() -> None:
    mod = importlib.import_module("exel_processor_EQ")
    if hasattr(mod, "main") and callable(mod.main):
        mod.main()
    else:
        print("Ошибка: в модуле exel_processor_EQ не найдена функция main().")

def run_summator() -> None:
    mod = importlib.import_module("summator")
    if hasattr(mod, "main") and callable(mod.main):
        mod.main()
    else:
        print("Ошибка: в модуле summator не найдена функция main().")

def exit_program() -> None:
    print("Завершение работы. До встречи!")
    sys.exit(0)

def read_choice() -> str:
    try:
        return input(PROMPT).strip()
    except (EOFError, KeyboardInterrupt):
        print("\nЗавершение работы.")
        sys.exit(0)

def main() -> None:
    print(WELCOME)
    actions: Dict[str, Callable[[], None]] = {
        "0": exit_program,
        "1": run_traceid_processor,
        "2": run_exel_processor,
        "3": run_sql_generator,
        "4": run_exel_processor_EQ,  # новый шаг 4
        "5": run_summator,           # summator перенесён на шаг 5
    }
    while True:
        choice = read_choice()
        if choice in actions:
            actions[choice]()
            if choice != "0":
                print("\nВозврат в главное меню...\n")
                print(WELCOME)
        else:
            print("Некорректный ввод. Допустимые значения: 0, 1, 2, 3, 4 или 5. Повторите попытку.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("\nНеобработанная ошибка:", repr(e))
        traceback.print_exc()
        input("\nНажмите Enter для выхода...")
    else:
        input("\nНажмите Enter для выхода...")
