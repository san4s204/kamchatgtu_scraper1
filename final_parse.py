import json

input_file = 'final_parsed_data.json'
output_file = 'final_parsed_data_no_props.json'

with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

for entry in data:
    if entry.get("class") == "general_motors":
        props = entry.get("props", [])
        if props and isinstance(props, list) and len(props) > 0:
            prop = props[0]
            # Переносим нужные поля из props на верхний уровень
            entry["title"] = prop.get("title", "")
            entry["content"] = prop.get("content", "")
            entry["keywords"] = prop.get("keywords", [])
            # Удаляем props
            del entry["props"]

with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

print(f"Обработка завершена. Результат сохранён в {output_file}")
