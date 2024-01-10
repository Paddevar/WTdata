import pandas as pd
import ast
import numpy as np
import re
import datetime

def load_data(file_path):
    return pd.read_csv(file_path, header=None)

data = load_data(r"C:\Users\Ivar\OneDrive\Documenten\Working Talent\Project\data_template.csv") #adjust string to destination of the current file

data = data[0].to_list()
#Create one large string
string = ''
for i in range(len(data)):
    string += data[i]

# print(string.count('}]\"'))
# print(string.count('\"[{'))
# print(string.count('\'s'))

string = string.replace('\"\"','\"') #replace "" by "
string = string.replace('\'','\"') #replace ' by "
string = string.replace('\"s ',"'s ") #replace "s (caused by previous line) by 's
string = string.replace('\"[{','}]\"').split('}]\"') #[{ is start of list of books from specific author and }] end

string.pop() #last element of list is empty
authors = []
authors_original_string = []
books = [] #Will contain information about books and their respective authors
for i in range(len(string)):
    if i%2 == 0:
        author = string[i].strip(',\"').lstrip(',') #even numbers correspond to authors
        
        authors_original_string.append(author)
        
        temp = re.findall(r'\d+', author)
        res = list(map(int, temp))
        
        current_year = datetime.datetime.now().year #get current year
        author_birth_year_bool = False
        for element in res:
            if element >= 1700 and element <=current_year: #if the number that has been found is between 1700 and current year (2024), we assume it is the year of birth
                author_birth_year = element
                author_birth_year_bool = True #set boolean to true, since birth year for author exists
        
        
        author = author.split(',')
        authors_list = author.copy()
        
        author_name = author[0] #Assume that author name is first part of a string (works for current data)
        if '\"' in author_name:
            author_name = author_name.replace('\"',"\'")
        
        if author_birth_year_bool == True:
            author = {'author_name': author_name, 'author_birth_year': author_birth_year}
            authors.append(author)
        else:
            author = {'author_name': author_name, 'author_birth_year': np.nan}
            authors.append(author)

    else:
        string_temp = '[{' + string[i] + '}]' #odd numbers correspond to all the titles of a single author
        res = ast.literal_eval(string_temp) #create list of dictionaries from string
        
        for j in range(len(res)): #iterate over list of dictionaries, add to books
        
            #Additional property can contain any property
            if 'additional_property' in res[j]:
                additional_property = res[j]['additional_property'].split(':')
                key = additional_property[0]
                value = additional_property[1].lstrip()
                res[j][key] = value
                del res[j]['additional_property']
            
            #Additional info can contain any list of additional properties
            if 'additional_info' in res[j]:
                additional_info = res[j]['additional_info']#.split(':')
                for key in additional_info:
                    res[j][key] = additional_info[key]
                del res[j]['additional_info']
                
            books.append(res[j])
            
            #Add author birth year and author name to data
            res[j].update(author)

#%% Check all possible keys for books 
counter = 0
keys = []
for book in books:
    for key in book:
        if key not in keys:
            keys.append(key)
#%%
df_books = pd.DataFrame(books) 

df_books['pages'] = df_books['pages'].astype('float').fillna(0)
df_books['page_count'] = df_books['page_count'].astype('float').fillna(0)

#Currently solve columns for every case, might have to change if many mistakes are present
df_books['pages'] = df_books['pages'] + df_books['page_count']
df_books['pages'] = df_books['pages'].replace(0,np.nan)

df_books['ISBN_number'] = df_books['ISBN_number'].fillna('') + df_books['ISBN'].fillna('')

df_books['release_date'] = df_books['release_date'].fillna('') + df_books['reease_date'].fillna('')
df_books['release_date'] = df_books['release_date'].fillna('') + df_books['releaseDate'].fillna('')

df_books = df_books.drop(columns=['ISBN','reease_date','releaseDate','page_count'])

pages = df_books['pages'].unique()

#For the column format, change eBook, ebook etc. to e-book
df_books.loc[(df_books['format'].str[0] == 'e') & (df_books['format'].str[-3:-1] == 'oo'), 'format'] = 'ebook'

#Since author names cannot contain numbers, replace the numbers by their respective characters
df_books['author_name'] = df_books['author_name'].replace('1', 'i', regex=True)
df_books['author_name'] = df_books['author_name'].replace('3', 'e', regex=True)
df_books['author_name'] = df_books['author_name'].replace('4', 'a', regex=True)
df_books['author_name'] = df_books['author_name'].replace('@', 'a', regex=True)
df_books['author_name'] = df_books['author_name'].replace('0', 'o', regex=True)
#%%Create multiple columns from tag column which now contains a list as entries (1 for each tag) 
error #tags fixen
tags = df_books['tags'] #make lower case
unique_tags = sorted(set([x for inner in tags for x in inner]))

# import math
# import re

# def get_cosine(vec1, vec2):
#     intersection = set(vec1.keys()) & set(vec2.keys())
#     numerator = sum([vec1[x] * vec2[x] for x in intersection])

#     sum1 = sum([vec1[x] ** 2 for x in list(vec1.keys())])
#     sum2 = sum([vec2[x] ** 2 for x in list(vec2.keys())])
#     denominator = math.sqrt(sum1) * math.sqrt(sum2)

#     if not denominator:
#         return 0.0
#     else:
#         return float(numerator) / denominator

# cosine = get_cosine(unique_tags, unique_tags)
#%%
from textblob import TextBlob

def Correct(x):
    return TextBlob(x).correct()

df_books2 = df_books['description'].apply(Correct)

# def Correct_String(text):
#     result = ''
#     text = text.split(' ')
#     for i in range(len(text)):
#         text[i] = text[i].strip(',.?!;:()')
#         text[i] = Correct(text[i])
#         if i<len(text)-1:
#             result += str(text[i]) + ' '
#         else:
#             result += str(text[i]) + '.'
#     print(result)
#     return result

df_books['description_corrected'] = df_books['description'].apply(lambda x: Correct_String(x))
compare = df_books[['description','description_corrected']]
#%%

def save_to_csv(file_path):
    df_books.to_csv(file_path)
    
save_to_csv(r"C:\Users\Ivar\OneDrive\Documenten\Working Talent\Project\WTbackend\data_cleaned.csv") #adjust string to destination for the file