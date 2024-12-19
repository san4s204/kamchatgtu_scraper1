import json

input_file = 'parsed_data_cleaned_v2.json'
output_file = 'parsed_data_cleaned_longest.json'

with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

processed_data = []
for entry in data:
    props = entry.get("props", [])
    if not props:
        continue
    
    # Находим проп с самым длинным content
    longest_prop = max(props, key=lambda p: len(p.get("content", "")))
    
    # Создаём новый entry с одним самым длинным пропом
    new_entry = {
        "class": entry.get("class", ""),
        "id": entry.get("id", ""),
        "props": [longest_prop],
        "url": entry.get("url", "")
    }
    processed_data.append(new_entry)

with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(processed_data, f, ensure_ascii=False, indent=4)

print(f"Обработка завершена. Результат сохранён в {output_file}")
