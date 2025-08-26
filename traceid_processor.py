#!/usr/bin/env python3
from __future__ import annotations

import math
import sys
import time
import re
from pathlib import Path
from typing import Iterable, Iterator, Tuple

def read_trace_ids(file_path: Path) -> Iterator[str]:
    with file_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if s:
                yield s

def write_lines(file_path: Path, lines: Iterable[str], newline: bool = True) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as f:
        if newline:
            for line in lines:
                f.write(line)
                if not line.endswith("\n"):
                    f.write("\n")
        else:
            for line in lines:
                f.write(line)

def progress_bar(current: int, total: int, prefix: str = "", width: int = 30) -> None:
    pct = 0.0 if total <= 0 else min(100.0, (current / total) * 100.0)
    filled = int(width * pct / 100)
    bar = "#" * filled + "." * (width - filled)
    sys.stdout.write(f"\r{prefix}[{bar}] {pct:5.1f}%")
    sys.stdout.flush()

def ask_inputs() -> Tuple[Path, str, Path]:
    directory_str = input("Введите путь к директории: ").strip()
    directory = Path(directory_str).expanduser().resolve()
    if not directory.is_dir():
        print("Указанная директория не существует.")
        sys.exit(1)

    filename_no_ext = input("Введите имя файла без расширения: ").strip()
    if not filename_no_ext:
        print("Имя файла не должно быть пустым.")
        sys.exit(1)

    filename = filename_no_ext + ".txt"
    source_file = directory / filename
    if not source_file.is_file():
        print(f"Файл не найден: {source_file}")
        sys.exit(1)

    return directory, filename_no_ext, source_file

# Новая функция: убрать ЛЮБЫЕ пробелы/пробельные символы внутри traceId.
def normalize_trace_id(s: str) -> str:
    # Удаляем все виды пробельных символов (пробелы, табы, \r, \n и т.п.)
    # Если пробелов нет, строка останется неизменной.
    return re.sub(r"\s+", "", s)

def unique_trace_ids_with_progress(file_path: Path) -> Tuple[Tuple[str, ...], int]:
    print("Выполняется поиск дублей записей...")
    total_lines = 0
    with file_path.open("r", encoding="utf-8", errors="ignore") as f:
        buf = f.read(1024 * 1024)
        while buf:
            total_lines += buf.count("\n")
            buf = f.read(1024 * 1024)

    seen: set[str] = set()
    duplicates = 0
    idx = 0
    with file_path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            idx += 1
            if idx % 1000 == 0 or idx == total_lines:
                progress_bar(idx, max(total_lines, 1), prefix="Поиск дублей: ")
            s = raw.strip()
            if not s:
                continue
            s = normalize_trace_id(s)  # нормализация перед учётом и дедупликацией
            if not s:
                continue
            if s in seen:
                duplicates += 1
            else:
                seen.add(s)

    sys.stdout.write("\n")
    if duplicates == 0:
        print("Массив подготовлен. Дублей не обнаружено")
    else:
        print(f"Массив подготовлен. Было найдено {duplicates} дублей.")
    return tuple(seen), duplicates

def to_group_dsl(trace_ids: Iterable[str]) -> Iterator[str]:
    for tid in trace_ids:
        tid_clean = normalize_trace_id(tid)  # идемпотентно, защита на всякий случай
        if not tid_clean:
            continue
        yield "{"
        yield '  "match_phrase": {'
        yield f'    "traceId": "{tid_clean}"'
        yield "  }"
        yield "},"

def save_first_proc(directory: Path, base_name: str, dsl_lines: Iterable[str]) -> Path:
    out_file = directory / f"{base_name}firstProc.txt"
    write_lines(out_file, dsl_lines)
    return out_file

def chunk_first_proc_into_groups(first_proc_file: Path, directory: Path, count_objects: int) -> int:
    objects_per_group = 3500
    groups_count = math.ceil(count_objects / objects_per_group) if count_objects > 0 else 0

    output_dir = directory / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Создаём пустые файлы заранее
    for i in range(1, groups_count + 1):
        (output_dir / f"dsl_group-{i}.txt").touch()

    if groups_count == 0:
        return 0

    current_group = 1
    objects_in_current_group = 0
    lines_buffer: list[str] = []

    def flush_group():
        nonlocal lines_buffer, current_group
        if lines_buffer:
            target = output_dir / f"dsl_group-{current_group}.txt"
            write_lines(target, lines_buffer)
            lines_buffer = []

    with first_proc_file.open("r", encoding="utf-8") as f:
        object_lines: list[str] = []
        for line in f:
            object_lines.append(line.rstrip("\n"))
            # Каждый объект — 5 строк:
            # {
            #   "match_phrase": {
            #     "traceId": "<...>"
            #   }
            # },
            if len(object_lines) == 5:
                if objects_in_current_group >= objects_per_group:
                    flush_group()
                    current_group += 1
                    objects_in_current_group = 0
                lines_buffer.extend(object_lines)
                objects_in_current_group += 1
                object_lines = []
        flush_group()

    return groups_count

PREFIX = """{
  "query": {
    "bool": {
      "should": [
"""
SUFFIX = """  ],
      "minimum_should_match": 1
    }
  }
}
"""

def wrap_group_file(file_path: Path) -> None:
    lines = []
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            lines.append(line.rstrip("\n"))

    # Убираем финальную запятую у последнего объекта в массиве should
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].strip() == "},":
            lines[i] = "}"
            break

    out_lines = []
    out_lines.extend(PREFIX.splitlines())
    out_lines.extend(lines)
    out_lines.extend(SUFFIX.splitlines())
    write_lines(file_path, out_lines)

def last_process_all_groups(directory: Path, groups_count: int) -> None:
    output_dir = directory / "output"
    for i in range(1, groups_count + 1):
        progress_bar(i - 1, groups_count, prefix="Обёртка файлов: ")
        file_path = output_dir / f"dsl_group-{i}.txt"
        wrap_group_file(file_path)
        time.sleep(0.005)
    progress_bar(groups_count, groups_count, prefix="Обёртка файлов: ")
    sys.stdout.write("\n")

def main():
    try:
        directory, filename_no_ext, source_file = ask_inputs()
        unique_ids, _ = unique_trace_ids_with_progress(source_file)
        ids_tuple = tuple(unique_ids)
        Count = len(ids_tuple)
        print(f"Всего уникальных объектов: {Count}")
        first_proc_path = save_first_proc(directory, filename_no_ext, to_group_dsl(ids_tuple))
        groupsCount = chunk_first_proc_into_groups(first_proc_path, directory, Count)
        if groupsCount > 0:
            last_process_all_groups(directory, groupsCount)
        print(f"Преобразование файлов завершено. Всего - {groupsCount} файлов.")
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")

if __name__ == "__main__":
    main()
