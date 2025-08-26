#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterator, Tuple
import configparser

def progress_bar(current: int, total: int, prefix: str = "", width: int = 30) -> None:
    if total <= 0:
        pct = 0.0
    else:
        pct = min(100.0, (current / total) * 100.0)
    filled = int(width * pct / 100)
    bar = "#" * filled + "." * (width - filled)
    sys.stdout.write(f"\r{prefix}[{bar}] {pct:5.1f}%")
    sys.stdout.flush()

def ask_directory() -> Path:
    directory_str = input("Укажите путь директории файлу с готовыми сущностями SQL: ").strip()
    directory = Path(directory_str).expanduser().resolve()
    if not directory.exists():
        print("Указан неверный путь. Повторите ввод.")
        return ask_directory()
    if directory.is_file():
        return directory.parent
    if not directory.is_dir():
        print("Указанная директория недоступна. Повторите ввод.")
        return ask_directory()
    return directory

def ask_filename_no_ext() -> str:
    name = input("Укажите имя файла без расширения: ").strip()
    if not name:
        print("Имя файла не может быть пустым. Повторите ввод.")
        return ask_filename_no_ext()
    return name

def ask_unit_and_filter() -> Tuple[str, str, str]:
    unit = input("Укажите юнит EQ: ").strip()
    if not unit:
        print("Юнит не может быть пустым. Повторите ввод.")
        return ask_unit_and_filter()
    schema1 = f"AFIL{unit}"
    schema2 = f"KFIL{unit}"
    filter_key = input("Укажите параметр для фильтрации в запросе SQL (например, F0UCUS1, F0UIDPL, F0UEAN): ").strip()
    if not filter_key:
        print("Имя параметра не может быть пустым. Повторите ввод.")
        return ask_unit_and_filter()
    return schema1, schema2, filter_key

# ---------- Конфигурация (INI) ----------

DEFAULT_PREFIX_TEMPLATE = (
    "SELECT F0UCUS1, F0UIDPL, F0UEAN, F0USCRD, F0USCON\n\n"
    "FROM {schema1}.F0UPF\n\n"
    "INNER JOIN {schema2}.GFPF ON F0UCUS1=GFCUS\n\n"
    "WHERE 1=1\n\n"
    "AND {filter} IN (\n"
)

def load_prefix_template(config_path: Path) -> str:
    """
    Загружает шаблон SQL-префикса из INI.
    Ожидается [sql]/prefix_template с плейсхолдерами {schema1}, {schema2}, {filter}.
    При отсутствии/ошибке — возвращает встроенный шаблон.
    """
    cp = configparser.ConfigParser()
    try:
        if not config_path.is_file():
            return DEFAULT_PREFIX_TEMPLATE
        with config_path.open("r", encoding="utf-8") as f:
            cp.read_file(f)
        tpl = cp.get("sql", "prefix_template", fallback="").strip("\n")
        if not tpl.strip():
            return DEFAULT_PREFIX_TEMPLATE
        return tpl.replace("\r\n", "\n").replace("\r", "\n")
    except Exception:
        return DEFAULT_PREFIX_TEMPLATE

def build_sql_prefix_from_template(schema1: str, schema2: str, filter_key: str, template: str) -> str:
    try:
        return template.format(schema1=schema1, schema2=schema2, filter=filter_key)
    except KeyError as e:
        print(f"В шаблоне отсутствует плейсхолдер {e}. Используется стандартный шаблон.")
        return DEFAULT_PREFIX_TEMPLATE.format(schema1=schema1, schema2=schema2, filter=filter_key)

def build_sql_postfix() -> str:
    # Постфикс с точкой с запятой на новой строке
    return ")\n;\n"

def read_values(src_file: Path) -> Iterator[str]:
    with src_file.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if s:
                yield s

def quote_value(val: str) -> str:
    safe = val.replace("'", "''")
    return f"'{safe}'"

def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write(content)

def main():
    try:
        # INI рядом с модулем
        module_dir = Path(__file__).resolve().parent
        config_path = module_dir / "sql_generator.ini"

        directory_sql = ask_directory()
        sql_filename_no_ext = ask_filename_no_ext()
        schema1, schema2, filter_key = ask_unit_and_filter()

        src_file = directory_sql / f"{sql_filename_no_ext}.txt"
        if not src_file.is_file():
            print(f"Файл не найден: {src_file}")
            return

        # Читаем значения и делаем бэкап
        values = list(read_values(src_file))
        backup_file = src_file.with_name(f"original{src_file.name}")
        try:
            original_text = src_file.read_text(encoding="utf-8", errors="ignore")
            backup_file.write_text(original_text, encoding="utf-8")
        except FileNotFoundError:
            pass

        # Подготовка: каждое значение в виде 'value',\n
        prepared_lines = [quote_value(v) + ",\n" for v in values]
        write_text(src_file, "".join(prepared_lines))

        # Префикс из INI и постфикс
        prefix_template = load_prefix_template(config_path)
        sql_query_pref = build_sql_prefix_from_template(schema1, schema2, filter_key, prefix_template)
        sql_query_post = build_sql_postfix()

        # Печать предпросмотра финального SQL (без списка значений, с многоточием)
        # Выводим ровно тот префикс, который будет использоваться, и постфикс, а внутри IN — ...
        preview = sql_query_pref + "  ...\n" + sql_query_post
        print("\nПредпросмотр SQL-запроса (используемый шаблон):\n")
        print(preview)

        # Лимиты
        MAX_TOTAL = 800
        RESERVED = 300
        MAX_BODY = MAX_TOTAL - RESERVED
        if MAX_BODY <= 0:
            print("Неверная конфигурация лимита символов.")
            return

        # Считываем подготовленные элементы 'value', (с запятой) и чистим запятую
        raw_items = []
        with src_file.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                if s.endswith(","):
                    s = s[:-1].rstrip()
                raw_items.append(s)  # 'value'

        # Формируем файлы: значения в одну строку, разделитель ", "
        count_sql = 0
        used = 0
        total = len(raw_items)

        while used < total:
            count_sql += 1
            current_items: list[str] = []
            current_len = 0

            while used < total:
                candidate = raw_items[used]
                add_len = len(candidate) if not current_items else len(candidate) + 2
                if current_len + add_len <= MAX_BODY:
                    current_items.append(candidate)
                    current_len += add_len
                    used += 1
                else:
                    break

            body_line = (", ".join(current_items) + "\n") if current_items else ""
            content = sql_query_pref + body_line + sql_query_post

            if len(content) > MAX_TOTAL and current_items:
                current_items.pop()
                body_line = (", ".join(current_items) + "\n") if current_items else ""
                content = sql_query_pref + body_line + sql_query_post

            out_path = directory_sql / f"{sql_filename_no_ext}-{count_sql}.txt"
            write_text(out_path, content)

            progress_bar(used, total, prefix="Подготовка файлов SQL: ")

        progress_bar(total, total, prefix="Подготовка файлов SQL: ")
        sys.stdout.write("\n")

        print(f"Подготовка файлов SQL-запросов завершена. Подготовлено {count_sql} файлов.")

    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")

if __name__ == "__main__":
    main()
