def parse_raw_data(path) -> list[str]:

    raw_data = text_file_to_string(path)
    cleaned_raw_data = clean_raw_string(raw_data)
    split_cleaned_raw_data = split_raw_string(cleaned_raw_data)

    return split_cleaned_raw_data


def text_file_to_string(path) -> str:

    with open(path, 'r') as file:
        string = file.read()

    return string


def clean_raw_string(string: str) -> str:

    replacements = {'\"\"': '\"', '\'': '\"', '\"s ': "'s ", '\"[{': '}]\"'}
    string_replaced = replace_values_in_string(string, replacements)

    return string_replaced


def replace_values_in_string(string: str, replacements: dict) -> str:

    for old, new in replacements.items():
        string = string.replace(old, new)

    return string


def split_raw_string(string: str) -> list[str]:

    split_spring = string.split('}]\"')
    split_spring.pop()

    return split_spring
