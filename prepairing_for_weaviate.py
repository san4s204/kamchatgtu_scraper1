import json
import os

input_file = 'final_parsed_data_general.json'  # Исходный файл
output_file = 'final_parsed_data_general_cleaned.json'  # Файл для сохранения результата

with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

new_data = []
removed_count = 0

for entry in data:
    if entry.get("class") == "Document":
        props = entry.get("properties", {})
        file_path = props.get("file_path")
        if file_path and not os.path.exists(file_path):
            # Файл не существует, пропускаем добавление этого entry
            removed_count += 1
            continue
    # Если это не Document или файл существует, добавляем обратно
    new_data.append(entry)

with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(new_data, f, ensure_ascii=False, indent=4)

print(f"Удалено {removed_count} блоков с отсутствующими документами.")
