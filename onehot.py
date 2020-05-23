
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import LabelEncoder

table_names = ('aka_name','aka_title','cast_info','char_name','comp_cast_type','company_name','company_type','complete_cast','info_type','keyword','kind_type','link_type','movie_companies','movie_info','movie_info_idx','movie_keyword','movie_link','name','person_info','role_type','title')
table_df = pd.DataFrame(table_names, columns=['Table Names'])
labelencoder = LabelEncoder()
table_df['table_names_Cat'] = labelencoder.fit_transform(table_df['Table Names'])

enc = OneHotEncoder(handle_unknown='ignore')
enc_df = pd.DataFrame(enc.fit_transform(table_df[['table_names_Cat']]).toarray())
table_df = table_df.join(enc_df)
del table_df['table_names_Cat']
print(table_df)
table_df.to_csv('onehottables.csv',index=False)

column_names = ('aka_name.id','aka_name.name','aka_name.name_pcode_cf','aka_name.person_id','aka_title.episode_nr','aka_title.episode_of_id','aka_title.id','aka_title.kind_id','aka_title.movie_id','aka_title.note','aka_title.production_year','aka_title.season_nr','aka_title.title','cast_info.id','cast_info.movie_id','cast_info.note','cast_info.person_id','cast_info.person_role_id','cast_info.role_id','char_name.id','char_name.name','comp_cast_type.id','comp_cast_type.kind','company_name.country_code','company_name.id','company_name.name','company_type.id','company_type.kind','complete_cast.id','complete_cast.movie_id','complete_cast.status_id','complete_cast.subject_id','info_type.id','info_type.info','keyword.id','keyword.keyword','kind_type.id','kind_type.kind','link_type.id','link_type.link','movie_companies.company_id','movie_companies.company_type_id','movie_companies.id','movie_companies.movie_id','movie_companies.note','movie_info.id','movie_info.info','movie_info.info_type_id','movie_info.movie_id','movie_info.note','movie_info_idx.id','movie_info_idx.info','movie_info_idx.info_type_id','movie_info_idx.movie_id','movie_info_idx.note','movie_keyword.id','movie_keyword.keyword_id','movie_keyword.movie_id','movie_link.id','movie_link.link_type_id','movie_link.linked_movie_id','movie_link.movie_id','name.gender','name.id','name.name','name.name_pcode_cf','person_info.id','person_info.info','person_info.info_type_id','person_info.note','person_info.person_id','role_type.id','role_type.role','title.episode_nr','title.episode_of_id','title.id','title.kind_id','title.production_year','title.season_nr','title.title')
column_df = pd.DataFrame(column_names, columns=['Column Names'])
labelencoder = LabelEncoder()
column_df['column_names_Cat'] = labelencoder.fit_transform(column_df['Column Names'])

enc = OneHotEncoder(handle_unknown='ignore')
enc_df = pd.DataFrame(enc.fit_transform(column_df[['column_names_Cat']]).toarray())
column_df = column_df.join(enc_df)
del column_df['column_names_Cat']
print(column_df)
column_df.to_csv('onehotcolumns.csv',index=False)

operator_names = ('!=','<','<=','=','>','>=','IN','IS','IS NOT','LIKE','NOT LIKE')
operator_df = pd.DataFrame(operator_names, columns=['Operator Names'])
labelencoder = LabelEncoder()
operator_df['operator_names_Cat'] = labelencoder.fit_transform(operator_df['Operator Names'])

enc = OneHotEncoder(handle_unknown='ignore')
enc_df = pd.DataFrame(enc.fit_transform(operator_df[['operator_names_Cat']]).toarray())
operator_df = operator_df.join(enc_df)
del operator_df['operator_names_Cat']
print(operator_df)
operator_df.to_csv('onehotoperators.csv',index=False)
