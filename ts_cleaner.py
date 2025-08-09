#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TS Cleaner - обработчик PDF файлов для организации по ISIN кодам.

Модуль для обработки PDF файлов с термшитами:
- Перемещение PDF из Data_in в Data_in/TS
- Нормализация имен файлов до формата ISIN.pdf
- Проверка соответствия ISIN в имени и содержимом
- Обработка дублей
- Архивирование и перенос в Data_work
"""

import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Iterable

try:
    import PyPDF2
except ImportError:
    print("Ошибка: PyPDF2 не установлен. Выполните: pip install PyPDF2")
    exit(1)

try:
    from rich.console import Console
except ImportError:
    print("Ошибка: rich не установлен. Выполните: pip install rich")
    exit(1)

# Глобальные настройки
console = Console()

# Абсолютные пути согласно ТЗ
PROJECT_ROOT = Path(r"F:\Python Projets\TS_Organizer")
DATA_IN = PROJECT_ROOT / "Data_in"
DATA_WORK = PROJECT_ROOT / "Data_work"
TS_ARCHIVE = PROJECT_ROOT / "TS_archive"

# Строгий паттерн ISIN
ISIN_PATTERN = re.compile(r'\b[A-Z]{2}[A-Z0-9]{9}\d\b')
# Расширенный паттерн для поиска ISIN с возможными разрывами
ISIN_FLEXIBLE_PATTERN = re.compile(r'\b[A-Z]{2}[\s\-]*[A-Z0-9][\s\-]*[A-Z0-9][\s\-]*[A-Z0-9][\s\-]*[A-Z0-9][\s\-]*[A-Z0-9][\s\-]*[A-Z0-9][\s\-]*[A-Z0-9][\s\-]*[A-Z0-9][\s\-]*[A-Z0-9][\s\-]*\d\b')


def ensure_ts_folder(data_in: Path) -> Path:
    """
    Создаёт и возвращает путь к папке Data_in/TS.
    
    Args:
        data_in: Путь к папке Data_in
        
    Returns:
        Path: Путь к созданной или существующей папке TS
        
    Raises:
        OSError: При невозможности создать папку
    """
    ts_folder = data_in / "TS"
    try:
        ts_folder.mkdir(exist_ok=True)
        console.print(f"[cyan]Папка TS подготовлена: {ts_folder}[/cyan]")
        return ts_folder
    except OSError as e:
        console.print(f"[red]Ошибка создания папки TS: {e}[/red]")
        raise


def list_pdfs_in_root(data_in: Path) -> list[Path]:
    """
    Возвращает список PDF файлов из корня Data_in (не рекурсивно).
    
    Args:
        data_in: Путь к папке Data_in
        
    Returns:
        list[Path]: Список путей к PDF файлам
    """
    try:
        pdf_files = [f for f in data_in.iterdir() 
                    if f.is_file() and f.suffix.lower() == '.pdf']
        console.print(f"[cyan]Найдено PDF файлов в корне Data_in: {len(pdf_files)}[/cyan]")
        return pdf_files
    except OSError as e:
        console.print(f"[red]Ошибка чтения папки Data_in: {e}[/red]")
        return []


def extract_isin_from_name(name: str) -> Optional[str]:
    """
    Пытается извлечь ISIN из имени файла.
    
    Удаляет пробелы/дефисы внутри кандидат-строки перед проверкой 
    строгим паттерном.
    
    Args:
        name: Имя файла (без расширения или с ним)
        
    Returns:
        Optional[str]: Найденный ISIN или None
    """
    # Убираем расширение если есть
    clean_name = Path(name).stem
    
    # Сначала ищем потенциальные ISIN с разрывами
    flexible_matches = ISIN_FLEXIBLE_PATTERN.findall(clean_name)
    
    for match in flexible_matches:
        # Нормализуем: убираем пробелы и дефисы
        normalized = re.sub(r'[\s\-]+', '', match)
        # Проверяем строгим паттерном
        if ISIN_PATTERN.match(normalized):
            return normalized
    
    # Если гибкий поиск не дал результатов, ищем прямые совпадения
    direct_matches = ISIN_PATTERN.findall(clean_name)
    if direct_matches:
        return direct_matches[0]
    
    return None


def extract_isin_from_pdf(pdf_path: Path) -> Optional[str]:
    """
    Читает текст PDF через PyPDF2 и возвращает первый валидный ISIN.
    
    Нормализует возможные разрывы внутри ISIN (убирает пробелы/дефисы).
    
    Args:
        pdf_path: Путь к PDF файлу
        
    Returns:
        Optional[str]: Найденный ISIN или None
    """
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            text = ""
            
            # Извлекаем текст со всех страниц
            for page_num in range(len(reader.pages)):
                try:
                    page = reader.pages[page_num]
                    text += page.extract_text() + " "
                except Exception as e:
                    console.print(f"[yellow]Предупреждение: ошибка чтения страницы {page_num + 1} из {pdf_path.name}: {e}[/yellow]")
                    continue
            
            if not text.strip():
                console.print(f"[yellow]Предупреждение: не удалось извлечь текст из {pdf_path.name}[/yellow]")
                return None
            
            # Сначала ищем потенциальные ISIN с разрывами
            flexible_matches = ISIN_FLEXIBLE_PATTERN.findall(text)
            
            for match in flexible_matches:
                # Нормализуем: убираем пробелы и дефисы
                normalized = re.sub(r'[\s\-]+', '', match)
                # Проверяем строгим паттерном
                if ISIN_PATTERN.match(normalized):
                    return normalized
            
            # Если гибкий поиск не дал результатов, ищем прямые совпадения
            direct_matches = ISIN_PATTERN.findall(text)
            if direct_matches:
                return direct_matches[0]
            
            return None
            
    except Exception as e:
        console.print(f"[red]Ошибка чтения PDF {pdf_path.name}: {e}[/red]")
        return None


def move_pdf_with_conflict_prompt(src: Path, dst_dir: Path) -> Optional[Path]:
    """
    Перемещает файл в dst_dir с обработкой конфликтов имён.
    
    При конфликте имени задаёт интерактивный вопрос (y/n) с показом ISIN 
    обоих файлов. При y удаляет новый файл, при n пропускает перенос.
    
    Args:
        src: Путь к исходному файлу
        dst_dir: Папка назначения
        
    Returns:
        Optional[Path]: Фактический путь нового расположения или None
    """
    dst_path = dst_dir / src.name
    
    if not dst_path.exists():
        try:
            shutil.move(str(src), str(dst_path))
            return dst_path
        except Exception as e:
            console.print(f"[red]Ошибка перемещения {src.name}: {e}[/red]")
            return None
    
    # Конфликт имён - показываем ISIN обоих файлов
    existing_isin = extract_isin_from_name(dst_path.name) or "ISIN не определён"
    new_isin = extract_isin_from_name(src.name) or "ISIN не определён"
    
    console.print(f"[yellow]Конфликт имён для файла: {src.name}[/yellow]")
    console.print(f"[yellow]Существующий файл ISIN: {existing_isin}[/yellow]")
    console.print(f"[yellow]Новый файл ISIN: {new_isin}[/yellow]")
    
    response = input("Удалить новый конфликтующий файл? (y/n): ").strip().lower()
    
    if response == 'y':
        try:
            src.unlink()
            console.print(f"[green]Новый файл {src.name} удалён[/green]")
            return None
        except Exception as e:
            console.print(f"[red]Ошибка удаления файла {src.name}: {e}[/red]")
            return None
    else:
        console.print(f"[cyan]Файл {src.name} пропущен[/cyan]")
        return None


def normalize_filenames_in_ts(ts_dir: Path) -> int:
    """
    Переименовывает все PDF в TS так, чтобы имя было ISIN.pdf.
    
    Args:
        ts_dir: Путь к папке TS
        
    Returns:
        int: Количество обработанных файлов
    """
    processed_count = 0
    
    try:
        pdf_files = [f for f in ts_dir.iterdir() 
                    if f.is_file() and f.suffix.lower() == '.pdf']
        
        for pdf_file in pdf_files:
            try:
                isin = extract_isin_from_name(pdf_file.name)
                if not isin:
                    console.print(f"[yellow]Предупреждение: ISIN не найден в имени файла {pdf_file.name}[/yellow]")
                    processed_count += 1
                    continue
                
                target_name = f"{isin}.pdf"
                
                # Если имя уже корректное
                if pdf_file.name.lower() == target_name.lower():
                    processed_count += 1
                    continue
                
                target_path = ts_dir / target_name
                
                # Обработка конфликта имён при переименовании
                if target_path.exists() and target_path != pdf_file:
                    suffix = 1
                    while target_path.exists():
                        target_name = f"{isin}_{suffix}.pdf"
                        target_path = ts_dir / target_name
                        suffix += 1
                
                pdf_file.rename(target_path)
                console.print(f"[green]Переименован: {pdf_file.name} -> {target_name}[/green]")
                processed_count += 1
                
            except Exception as e:
                console.print(f"[red]Ошибка переименования {pdf_file.name}: {e}[/red]")
                processed_count += 1
                continue
                
    except Exception as e:
        console.print(f"[red]Ошибка обработки папки TS: {e}[/red]")
    
    return processed_count


def interactive_fix_mismatched_isin(ts_dir: Path) -> None:
    """
    Сверяет ISIN из имени и из содержимого PDF.
    
    При расхождении задаёт вопрос (y/n) и при согласии переименовывает
    файл в соответствии с ISIN из содержимого.
    
    Args:
        ts_dir: Путь к папке TS
    """
    try:
        pdf_files = [f for f in ts_dir.iterdir() 
                    if f.is_file() and f.suffix.lower() == '.pdf']
        
        for pdf_file in pdf_files:
            try:
                name_isin = extract_isin_from_name(pdf_file.name)
                content_isin = extract_isin_from_pdf(pdf_file)
                
                if not name_isin:
                    console.print(f"[yellow]Файл {pdf_file.name}: ISIN в имени не определён[/yellow]")
                    continue
                
                if not content_isin:
                    console.print(f"[yellow]Файл {pdf_file.name}: ISIN в содержимом не найден[/yellow]")
                    continue
                
                if name_isin != content_isin:
                    console.print(f"[yellow]Обнаружено несоответствие: имя файла {name_isin}, в TermSheet указан {content_isin}[/yellow]")
                    response = input("Заменить имя на ISIN из TermSheet? (y/n): ").strip().lower()
                    
                    if response == 'y':
                        target_name = f"{content_isin}.pdf"
                        target_path = ts_dir / target_name
                        
                        # Обработка конфликта имён
                        if target_path.exists() and target_path != pdf_file:
                            suffix = 1
                            while target_path.exists():
                                target_name = f"{content_isin}_{suffix}.pdf"
                                target_path = ts_dir / target_name
                                suffix += 1
                        
                        pdf_file.rename(target_path)
                        console.print(f"[green]Переименован: {pdf_file.name} -> {target_name}[/green]")
                    else:
                        console.print(f"[cyan]Файл {pdf_file.name} оставлен без изменений[/cyan]")
                        
            except Exception as e:
                console.print(f"[red]Ошибка обработки файла {pdf_file.name}: {e}[/red]")
                continue
                
    except Exception as e:
        console.print(f"[red]Ошибка сверки ISIN: {e}[/red]")


def find_duplicates_by_isin(ts_dir: Path) -> dict[str, list[Path]]:
    """
    Ищет файлы-дубли по одному и тому же ISIN в имени.
    
    Args:
        ts_dir: Путь к папке TS
        
    Returns:
        dict[str, list[Path]]: Словарь ISIN -> список путей дублей
    """
    isin_files: dict[str, list[Path]] = {}
    
    try:
        pdf_files = [f for f in ts_dir.iterdir() 
                    if f.is_file() and f.suffix.lower() == '.pdf']
        
        for pdf_file in pdf_files:
            isin = extract_isin_from_name(pdf_file.name)
            if isin:
                if isin not in isin_files:
                    isin_files[isin] = []
                isin_files[isin].append(pdf_file)
        
        # Возвращаем только дубли (где больше одного файла)
        duplicates = {isin: files for isin, files in isin_files.items() if len(files) > 1}
        
        if duplicates:
            console.print(f"[yellow]Найдено дублей по ISIN: {len(duplicates)}[/yellow]")
        else:
            console.print("[green]Дублей не найдено[/green]")
            
        return duplicates
        
    except Exception as e:
        console.print(f"[red]Ошибка поиска дублей: {e}[/red]")
        return {}


def interactive_remove_duplicates(dups: dict[str, list[Path]]) -> None:
    """
    Для каждой группы дублей спрашивает по каждому лишнему файлу (y/n).
    
    Args:
        dups: Словарь ISIN -> список путей дублей
    """
    for isin, files in dups.items():
        console.print(f"[yellow]Обработка дублей для ISIN: {isin}[/yellow]")
        console.print(f"[cyan]Найдено файлов: {len(files)}[/cyan]")
        
        for i, file_path in enumerate(files):
            console.print(f"[cyan]Файл {i + 1}: {file_path.name}[/cyan]")
            
            # Оставляем первый файл, остальные предлагаем удалить
            if i == 0:
                console.print("[green]Первый файл оставляем[/green]")
                continue
                
            response = input(f"Удалить файл {file_path.name}? (y/n): ").strip().lower()
            
            if response == 'y':
                try:
                    file_path.unlink()
                    console.print(f"[green]Файл {file_path.name} удалён[/green]")
                except Exception as e:
                    console.print(f"[red]Ошибка удаления файла {file_path.name}: {e}[/red]")
            else:
                console.print(f"[cyan]Файл {file_path.name} оставлен[/cyan]")


def move_ts_to_data_work_with_archive(data_in_ts: Path, data_work: Path, archive_root: Path) -> None:
    """
    Переносит Data_in/TS в Data_work/TS с архивированием.
    
    Если уже есть TS в Data_work - спрашивает (y/n), при согласии 
    архивирует старую TS в TS_archive/TS_dd_mm_yyyy, затем переносит новую.
    
    Args:
        data_in_ts: Путь к папке Data_in/TS
        data_work: Путь к папке Data_work
        archive_root: Путь к корню архива
    """
    try:
        # Создаём Data_work если не существует
        data_work.mkdir(exist_ok=True)
        
        data_work_ts = data_work / "TS"
        
        if not data_work_ts.exists():
            # Простое перемещение
            shutil.move(str(data_in_ts), str(data_work_ts))
            console.print(f"[green]Папка TS успешно перемещена в Data_work[/green]")
            return
        
        # Конфликт - папка уже существует
        console.print("[yellow]Папка Data_work уже содержит папку TS[/yellow]")
        response = input("Заменить существующую папку? (y/n): ").strip().lower()
        
        if response != 'y':
            console.print("[cyan]Операция отменена[/cyan]")
            return
        
        # Архивирование существующей папки
        archive_root.mkdir(exist_ok=True)
        current_date = datetime.now().strftime("%d_%m_%Y")
        archive_name = f"TS_{current_date}"
        archive_path = archive_root / archive_name
        
        # Обработка конфликта имён в архиве
        if archive_path.exists():
            suffix = 1
            while archive_path.exists():
                archive_name = f"TS_{current_date}_{suffix}"
                archive_path = archive_root / archive_name
                suffix += 1
        
        # Перемещение в архив
        shutil.move(str(data_work_ts), str(archive_path))
        console.print(f"[green]Существующая папка TS заархивирована: {archive_name}[/green]")
        
        # Перемещение новой папки
        shutil.move(str(data_in_ts), str(data_work_ts))
        console.print(f"[green]Новая папка TS перемещена в Data_work[/green]")
        
    except Exception as e:
        console.print(f"[red]Ошибка перемещения папки TS: {e}[/red]")


def main() -> None:
    """
    Оркестрация всех шагов обработки PDF файлов.
    
    Выполняет полный конвейер:
    1. Подготовка папки TS
    2. Перенос PDF файлов
    3. Нормализация имён
    4. Сверка ISIN
    5. Обработка дублей
    6. Финальный перенос в Data_work
    """
    console.print("[bold cyan]Запуск TS Cleaner[/bold cyan]")
    console.print(f"[cyan]Рабочая папка: {PROJECT_ROOT}[/cyan]")
    
    try:
        # Шаг 1: Создание папки TS
        console.print("\n[bold cyan]Шаг 1: Подготовка папки TS[/bold cyan]")
        ts_folder = ensure_ts_folder(DATA_IN)
        
        # Шаг 2: Перенос PDF файлов
        console.print("\n[bold cyan]Шаг 2: Перенос PDF файлов[/bold cyan]")
        pdf_files = list_pdfs_in_root(DATA_IN)
        
        moved_count = 0
        for pdf_file in pdf_files:
            result = move_pdf_with_conflict_prompt(pdf_file, ts_folder)
            if result:
                moved_count += 1
        
        console.print(f"[green]Перемещено файлов: {moved_count}[/green]")
        
        # Шаг 3: Нормализация имён
        console.print("\n[bold cyan]Шаг 3: Нормализация имён файлов[/bold cyan]")
        processed_count = normalize_filenames_in_ts(ts_folder)
        console.print(f"[green]Обработано файлов: {processed_count}[/green]")
        
        # Шаг 4: Сверка ISIN
        console.print("\n[bold cyan]Шаг 4: Проверка соответствия ISIN[/bold cyan]")
        interactive_fix_mismatched_isin(ts_folder)
        
        # Шаг 5: Поиск и обработка дублей
        console.print("\n[bold cyan]Шаг 5: Поиск дублей[/bold cyan]")
        duplicates = find_duplicates_by_isin(ts_folder)
        if duplicates:
            console.print("\n[bold cyan]Обработка дублей[/bold cyan]")
            interactive_remove_duplicates(duplicates)
        
        # Итоговая статистика
        final_files = [f for f in ts_folder.iterdir() 
                      if f.is_file() and f.suffix.lower() == '.pdf']
        console.print(f"\n[bold green]Итого файлов в папке TS: {len(final_files)}[/bold green]")
        
        # Шаг 6: Перенос в Data_work
        console.print("\n[bold cyan]Шаг 6: Перенос в Data_work[/bold cyan]")
        move_ts_to_data_work_with_archive(ts_folder, DATA_WORK, TS_ARCHIVE)
        
        console.print("\n[bold green]Обработка завершена успешно![/bold green]")
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Операция прервана пользователем[/yellow]")
    except Exception as e:
        console.print(f"\n[red]Критическая ошибка: {e}[/red]")


if __name__ == "__main__":
    main()
