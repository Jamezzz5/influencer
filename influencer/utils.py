import os
import logging

config_path = 'config/'


def dir_check():
    pass

def write_df(df, file_name, file_path=None):
    if file_path:
        file_name = os.path.join(file_path, file_name)
    df.to_excel(file_name, index=False)
    logging.info('Successfully wrote to {}.'.format(file_name))


def df_filter_by_word(df, cols, word, inverse=False):
    old_cols = df.columns
    for col in cols:
        new_col_name = '{} - {}'.format(col, word)
        df[new_col_name] = df[col].str.lower().str.contains(word.lower())
    word_mask = df[[x for x in df.columns if x not in old_cols]].any(axis=1)
    if inverse:
        df = df[~word_mask]
    else:
        df = df[word_mask]
    df = df[old_cols]
    return df
