from psycopg2 import sql, connect
from config import config
import glob,os
import re
import csv
import math
import fasttext
import json
from num2words import num2words
import pandas as pd

def gen_query_files():
    queries_for_psql = []
    queries_for_encoding = []

    allfiles = glob.glob('job' + '/*.sql', recursive=True)
    for queryfile in allfiles:
        filename = os.path.basename(queryfile)
        with open(queryfile, 'r') as file:
            query = file.read().splitlines()
            query = ','.join(query)
            query = ' '.join(query.split()).replace(',,',',').replace(', ',',')
            psql_query = re.sub("(SELECT(.+?)FROM)",'SELECT COUNT(*) FROM',query).replace(';,',';')
            psql_query = filename+'#'+psql_query
            queries_for_psql.append(psql_query)
            encoded_query = re.sub("(SELECT(.+?)FROM )",'',query).replace('AS ','').replace(',WHERE ','#').replace(',AND ','#').replace(';,','').replace(',OR',' OR')
            encoded_query = filename+'#'+encoded_query
            queries_for_encoding.append(encoded_query)

    with open('job/edited_queries/psql_queries.txt', 'w') as file:
        for item in sorted(queries_for_psql):
            file.write("%s\n" % item)

    with open('job/edited_queries/encoded_queries.txt', 'w') as file:
        for item in queries_for_encoding:
            file.write("%s\n" % item)

def get_cardi_info():
    all_card_dict = []
    with open('job/edited_queries/psql_queries.txt', 'r') as file:
        query_list = file.read().splitlines()
    params = config()
    conn = connect(**params)
    cur = conn.cursor()
    for query in query_list:
        query_blocks = query.split('#')
        q_num = query_blocks[0].strip()
        psql_query = query_blocks[1].strip()
        cur.execute(psql_query)
        result = cur.fetchone()
        result = result[0]
        if(result!=0):
            log_cardi = math.log10(result)
        else:
            log_cardi = 0.0
        all_card_dict.append( [q_num,result,log_cardi])
    with open('query_cardinalities.csv', 'w') as file:
        for item in all_card_dict:
            file.write("%s\n" % item)

def read_query_files():
    with open('job/edited_queries/encoded_queries.txt', 'r') as file:
        query = file.read().splitlines()
    return query

def get_query_blocks(query_set):
    data_dict = {}
    concat_arr = []
    query_counter = 0
    get_encoded_vectors()
    for query in query_set:
        internal_dict = {}
        _arr = []
        query_counter += 1
        query_blocks = query.split('#')
        filename = query_blocks[0]
        print([filename])
        _arr.append(query_counter)
        table_alias = {}
        table_set = []
        join_set = []
        predicate_set_num = []
        predicate_set_str = []
        cardinality_set = []
        query_for_samples = "SELECT id FROM"
        table_blocks = query_blocks[1].split(',')
        for table in table_blocks:
            table,sep,alias = table.partition(' ')
            table_set.append(tbl_dict[table])
            alias_key = ' '+alias+'.'
            if(alias_key not in table_alias.keys()):
                table_alias[' '+alias+'.'] = table
        internal_dict['Tables'] =  table_set
        _arr.append(table_set)
        join_blocks = query_blocks[3]
        join_pairs = join_blocks.split("|")
        for join in join_pairs:
            left_join,opr,right_join = join.partition('=')
            left_join = ' '+left_join.strip()
            right_join = ' '+right_join.strip()
            for key in table_alias.keys():
                if(key in left_join):
                    left_join = left_join.replace(key,table_alias[key]+'.').strip()
                if(key in right_join):
                    right_join = right_join.replace(key,table_alias[key]+'.').strip()
            if(left_join<right_join):
                encoded_join = col_dict[left_join] + opr_dict['='] + col_dict[right_join]
            elif(right_join<left_join):
                encoded_join = col_dict[right_join] + opr_dict['='] + col_dict[left_join]
            join_set.append(encoded_join)
        internal_dict['Joins'] =  join_set
        _arr.append(join_set)
        predicate_blocks = query_blocks[2]
        all_predicates = predicate_blocks.split('|')
        for predicate in all_predicates:
            if(predicate[0] == "(" and predicate[-1] == ")"):
                predicate = predicate[1:-1]
            if(' OR ' in predicate) or (' IN ' in predicate):
                or_predicate_blocks = []
                if(' OR ' in predicate):
                    or_blocks = predicate.split(' OR ')
                    or_predicate_blocks.extend(or_blocks)
                if(' IN ' in predicate):
                    # print(predicate)
                    col,op,vals = predicate.partition(' IN ')
                    vals = vals[1:-1]
                    all_vals = vals.split(',')
                    for val in all_vals:
                        or_blocks = col+' = '+val
                        or_predicate_blocks.append(or_blocks)
                for or_predicate_block in or_predicate_blocks:
                    get_predicate_encoding(or_predicate_block,table_alias,predicate_set_str,predicate_set_num)
            else:
                get_predicate_encoding(predicate,table_alias,predicate_set_str,predicate_set_num)

        internal_dict['Predicate String'] = predicate_set_str
        internal_dict['Predicate Numeric'] = predicate_set_num
        _arr.append(predicate_set_str)
        _arr.append(predicate_set_num)

        cardinality, log_cardinality = get_cardinality(filename)

        original_cardi_norm =(cardinality-min_cardinality)/(max_cardinality-min_cardinality)
        log_cardi_norm =(log_cardinality-min_log_cardinality)/(max_log_cardinality-min_log_cardinality)

        internal_dict['Cardinality'] = log_cardi_norm
        _arr.append(log_cardi_norm)

        data_dict[filename] = internal_dict
        concat_arr.append(_arr)
    write_to_file(data_dict)
    create_pd_df(concat_arr)

def create_pd_df(concat_arr):
    df = pd.DataFrame(concat_arr, columns=['Query Number','Tables','Joins','Predicate String','Predicate Numeric','Cardinality'])
    print(df)
    filepath = os.path.join('output_files','pd_encoded_job_onehot.csv')
    df.to_csv(filepath, sep='\t', encoding='utf-8', index=False)

def write_to_file(data_dict):
    filepath = os.path.join('output_files','encoded_job_onehot.json')
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data_dict, f, ensure_ascii=False, indent=4)

    with open('output_files/stringsInJobQueries.txt', 'w', encoding="utf-8") as f:
        for str_val in allStringVals:
            f.write("%s\n" % str_val)

def get_cardinality(filename):
    if filename in cardi_info.keys():
        cardi = cardi_info[filename][0]
        log_cardi = cardi_info[filename][1]
    return cardi, log_cardi

def get_predicate_encoding(predicate_block,table_alias,predicate_set_str,predicate_set_num):
    opr_list = ['!=','<=','>=','=','>','<','IS NOT','IS','NOT LIKE','LIKE']
    for opr in opr_list:
        if(opr in predicate_block):
            if(predicate_block[0]=='('):
                predicate_block = predicate_block[1:]
            if(predicate_block[-1]==')'):
                predicate_block = predicate_block[:-1]
            col,opr,val = predicate_block.partition(opr)
            col = ' '+col.strip()
            for key in table_alias.keys():
                if(key in col):
                    col = col.replace(key,table_alias[key]+'.').strip()
                    encoded_col = col_dict[col]
            op = opr.strip()
            encoded_opr = opr_dict[opr]
            val = val.strip()
            if(val[0]=='(' and val[-1]==')'):
                val = val[1:-1]
            if val.isdigit():
                encoded_val = get_normalized_val(col,val)
                temp_val = encoded_col+encoded_opr+[encoded_val]
                predicate_set_num.append(temp_val)
            else:
                if val =="''":
                    val="'NULL'"
                if(val=="'m'"):
                    val = "'male'"
                elif(val=="'f'"):
                    val = "'female'"
                encoded_val = get_wd_vector(val)
                temp_val = encoded_col+encoded_opr+encoded_val
                predicate_set_str.append(temp_val)
            break

def get_normalized_val(col,val):
    min_val = float(col_stats[col][0])
    max_val = float(col_stats[col][1])
    if max_val > min_val:
        norm_val = (float(val) - min_val) / (max_val - min_val)
    return norm_val

def get_wd_vector(val):
    cleaned_val = trim_words(val)
    embedded_vec = get_word_vecs(cleaned_val)
    return embedded_vec

def trim_words(pred_str):
    str_val = pred_str.strip()
    clean_string = re.sub('\n', '', str_val)
    clean_string = re.sub('[=;*\]\[:]', '', clean_string)
    trimmed_string = ' '.join(clean_string.split())
    final_string = get_numwords(trimmed_string)
    final_string = final_string.casefold().strip()
    final_string = final_string[1:-1]
    final_string = ' '.join(final_string.split())
    allStringVals.append(final_string)
    return final_string

def get_numwords(trimmed_string):
    num_list = re.findall(r'\d+',trimmed_string)
    if(num_list):
        for num in num_list:
            lit_num = num2words(num)
            trimmed_string = trimmed_string.replace(num,' '+lit_num)
    return trimmed_string

def get_word_vecs(trimmed_word):
    wd_vector = ft_model.get_sentence_vector(trimmed_word)
    wd_vector_list = wd_vector.tolist()
    return wd_vector_list

def get_encoded_vectors():
    with open('onehottables.csv', 'r') as file:
        reader = csv.reader(file, delimiter = ',')
        tbl_vectors = list(reader)
        for tbl in tbl_vectors:
            onehottables = [float(i) for i in tbl[1:]]
            tbl_dict[tbl[0]] = onehottables
    with open('onehotoperators.csv', 'r') as file:
        reader = csv.reader(file, delimiter = ',')
        opr_vectors = list(reader)
        for opr in opr_vectors:
            onehotoperators = [float(i) for i in opr[1:]]
            opr_dict[opr[0]] = onehotoperators
    with open('onehotcolumns.csv', 'r') as file:
        reader = csv.reader(file, delimiter = ',')
        col_vectors = list(reader)
        for col in col_vectors:
            onehotcolumns = [float(i) for i in col[1:]]
            col_dict[col[0]] = onehotcolumns
    with open('column_min_max_vals.csv', 'r') as file:
        reader = csv.reader(file, delimiter = ',')
        col_max_min_info = list(reader)
        for col in col_max_min_info:
            col_stats[col[0]] = (col[1],col[2])
    with open('query_cardinalities.csv', 'r') as file:
        global max_cardinality,min_cardinality,max_log_cardinality,min_log_cardinality
        max_cardinality = 0
        min_cardinality = 0
        max_log_cardinality = 0.0
        min_log_cardinality = 0.0
        reader = csv.reader(file, delimiter = ',')
        col_info = list(reader)
        cardi_list = []
        log_cardi_list = []
        for col in col_info:
            original_cardi = int(col[1])
            cardi_log = float(col[2])
            cardi_info[col[0]] = (original_cardi,cardi_log)
            cardi_list.append(original_cardi)
            log_cardi_list.append(cardi_log)

        max_cardinality = max(cardi_list)
        min_cardinality = min(cardi_list)

        max_log_cardinality = max(log_cardi_list)
        min_log_cardinality = min(log_cardi_list)

if __name__ == "__main__":
    global tbl_dict,col_dict,opr_dict,col_stats,cardi_info,ft_model,allStringVals
    tbl_dict = {}
    col_dict = {}
    opr_dict = {}
    col_stats = {}
    cardi_info = {}
    allStringVals = []
    # gen_query_files()
    # get_cardi_info()
    ft_model = fasttext.load_model('wiki.en.bin')
    query_set = read_query_files()
    get_query_blocks(query_set)
