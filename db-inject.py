import sys
import sqlite3
import time
import random
import statistics

def get_table_counts(db_filename):
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    # Получаем список таблиц в базе данных
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    table_counts = {}

    # Для каждой таблицы получаем количество записей
    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cursor.fetchone()[0]
        table_counts[table_name] = count

    conn.close()

    return table_counts

def get_table_structure(db_filename):
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    # Получаем список таблиц в базе данных
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    table_structure = {}

    # Для каждой таблицы получаем структуру и записи
    for table in tables:
        table_name = table[0]
        
        # Получаем структуру таблицы
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        # Получаем первую и последнюю записи таблицы
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1;")
        first_row = cursor.fetchone()
        
        cursor.execute(f"SELECT * FROM {table_name} ORDER BY ROWID DESC LIMIT 1;")
        last_row = cursor.fetchone()
        
        table_structure[table_name] = {
            "columns": columns,
            "first_row": first_row,
            "last_row": last_row
        }

    conn.close()

    return table_structure

def insert_record(db_filename, ts, short, min_val, max_val, med_val, avg_val, unit):
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    try:
        cursor.execute("INSERT INTO measurements (ts, short, min, max, med, avg, unit) VALUES (?, ?, ?, ?, ?, ?, ?);",
                       (ts, short, min_val, max_val, med_val, avg_val, unit))
        conn.commit()
        print("Запись для ", short, " успешно добавлена в таблицу measurements.")
    except sqlite3.Error as e:
        print("Ошибка при добавлении записи:", e)

    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python program.py <db_filename> [w]")
        sys.exit(1)
    
    db_filename = sys.argv[1]
    table_counts = get_table_counts(db_filename)
    table_structure = get_table_structure(db_filename)

    print("Список таблиц и количество записей:")
    for table_name, count in table_counts.items():
        print(f"{table_name}: {count} записей")
    print("=================")

    print("Структура таблиц и первая/последняя записи:")
    for table_name, table_info in table_structure.items():
        print(f"{table_name}:")
        
        columns = table_info["columns"]
        print("  Структура:")
        for column in columns:
            column_name = column[1]
            data_type = column[2]
            is_primary_key = "ПК" if column[5] == 1 else ""
            print(f"    {column_name} ({data_type}) {is_primary_key}")
        
        first_row = table_info["first_row"]
        last_row = table_info["last_row"]
        
        print("  Первая запись:")
        if first_row:
            print("    ", end="")
            print(*first_row, sep=", ")
        else:
            print("    Нет данных")
        
        print("  Последняя запись:")
        if last_row:
            print("    ", end="")
            print(*last_row, sep=", ")
        else:
            print("    Нет данных")


    if len(sys.argv) == 3 and sys.argv[2] == "w":
        short_list = [
                    "S0", "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8",
                    "S9", "S10", "S11", "S12", "S13", "S14", "S15", "S16", "S17"
                ]
        while (True):
            ts = (int(time.time()) * 1000) - 5000
            for short in short_list:
                random_vals = [random.uniform(-1000, 1000) for _ in range(5)]
                min_val = float(min(random_vals))
                max_val = float(max(random_vals))
                med_val = float(statistics.mean(random_vals))
                avg_val = float(statistics.median(random_vals))
                unit = "unit"  # Здесь можно задать единицу измерения
                
                insert_record(db_filename, ts, short, min_val, max_val, med_val, avg_val, unit)