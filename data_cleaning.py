# External imports
import pandas as pd
import ast
import numpy as np
import re
import datetime

# Internal imports
from input_parser import parse_raw_data


def text_to_df(string: str) -> pd.DataFrame:

    authors = []
    authors_original_string = []
    books = []  # Will contain information about books and their respective authors

    for i in range(len(string)):
        if i % 2 == 0:
            author = string[i].strip(',\"').lstrip(',')  # even numbers correspond to authors

            authors_original_string.append(author)

            temp = re.findall(r'\d+', author)
            res = list(map(int, temp))

            current_year = datetime.datetime.now().year  # get current year
            author_birth_year_bool = False
            for element in res:
                if element >= 1700 and element <= current_year:  # if the number that has been found is between 1700 and current year (2024), we assume it is the year of birth
                    author_birth_year = element
                    author_birth_year_bool = True  # set boolean to true, since birth year for author exists

            author = author.split(',')
            authors_list = author.copy()

            author_name = author[0]  # Assume that author name is first part of a string (works for current data)
            if '\"' in author_name:
                author_name = author_name.replace('\"', "\'")

            if author_birth_year_bool:
                author = {'author_name': author_name, 'author_birth_year': author_birth_year}
                authors.append(author)
            else:
                author = {'author_name': author_name, 'author_birth_year': np.nan}
                authors.append(author)

        else:
            string_temp = '[{' + string[i] + '}]'  # odd numbers correspond to all the titles of a single author
            res = ast.literal_eval(string_temp)  # create list of dictionaries from string

            for j in range(len(res)):  # iterate over list of dictionaries, add to books

                # Additional property can contain any property
                if 'additional_property' in res[j]:
                    additional_property = res[j]['additional_property'].split(':')
                    key = additional_property[0]
                    value = additional_property[1].lstrip()
                    res[j][key] = value
                    del res[j]['additional_property']

                # Additional info can contain any list of additional properties
                if 'additional_info' in res[j]:
                    additional_info = res[j]['additional_info']  # .split(':')
                    for key in additional_info:
                        res[j][key] = additional_info[key]
                    del res[j]['additional_info']

                books.append(res[j])

                # Add author birth year and author name to data
                res[j].update(author)

    # %% Check all possible keys for books
    counter = 0
    keys = []
    for book in books:
        for key in book:
            if key not in keys:
                keys.append(key)

    df_books = pd.DataFrame(books)

    return df_books


#Since author names cannot contain numbers, replace the numbers by their respective characters
df_books['author_name'] = df_books['author_name'].replace('1', 'i', regex=True)
df_books['author_name'] = df_books['author_name'].replace('3', 'e', regex=True)
df_books['author_name'] = df_books['author_name'].replace('4', 'a', regex=True)
df_books['author_name'] = df_books['author_name'].replace('@', 'a', regex=True)
df_books['author_name'] = df_books['author_name'].replace('0', 'o', regex=True)
#%%
def save_to_csv(file_path):
    df_books.to_csv(file_path)
    
save_to_csv(r"C:\Users\Ivar\OneDrive\Documenten\Working Talent\Project\WTbackend\data_cleaned.csv") #adjust string to destination for the file
#%%
from neuspell import BertChecker

checker = BertChecker()
checker.from_pretrained("./neuspell-subwordbert-probwordnoise/")
#%%
df_books['title_corrected'] = df_books['title'].apply(lambda x: checker.correct(x))
df_books['description_corrected'] = df_books['description'].apply(lambda x: checker.correct(x))
#%%
df_books = df_books.drop(columns=['title','description'])
df_books = df_books.rename(columns={'title_corrected': 'title', 'description_corrected': 'description'})
#%%
def fix_string(text):
    text = text.replace(' .', '.')
    text = text.replace(' \' s', '\'s')
    text = text.replace(' ,', ',')
    text = text.replace(' - ', '-')
    text = text.replace(' / ', '/')
    text = text.replace('C + +', 'C++')
    text = text.replace(' :', ':')
    return text

df_books['title'] = df_books['title'].apply(lambda x: fix_string(x))
df_books['description'] = df_books['description'].apply(lambda x: fix_string(x))

save_to_csv(r"C:\Users\Ivar\OneDrive\Documenten\Working Talent\Project\WTdata\data_corrected.csv")
