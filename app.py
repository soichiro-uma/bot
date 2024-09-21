import streamlit as st
import boto3
import pandas as pd
from io import StringIO

# data = pd.read_csv("20240919_all.csv",encoding='UTF-16')

##### データ読み込み
# S3のバケット名とCSVファイルのキー（パス）を指定
bucket_name = st.secrets["db_username"]
csv_file_key = st.secrets["db_password"]

# S3にアクセスするためのセッションを作成
s3_client = boto3.client('s3',
    aws_access_key_id= st.secrets["a"],
    aws_secret_access_key= st.secrets["b"],
    region_name=st.secrets["c"])

# S3からCSVファイルの内容を取得
csv_obj = s3_client.get_object(Bucket=bucket_name, Key=csv_file_key)

# StreamingBodyからデータを取得し、バイナリデータをUTF-16にデコード
body = csv_obj['Body'].read().decode('UTF-16')

# デコードされたデータをpandasのDataFrameに変換
data = pd.read_csv(StringIO(body))


##### サイドバー
st.sidebar.title('選択してください')
# 抽出
top_numbers = list(range(0, 4))
top_number = st.sidebar.selectbox("トップバイヤーの人数",top_numbers,index=3)
category_numbers = list(range(0, 11))
category_number = st.sidebar.selectbox("最近売れたアイテム数",category_numbers,index=10)
categorys = ['レディース', 'メンズ', 'ビューティ','ベビーキッズ', 'ホーム', 'スポーツ']
category = st.sidebar.selectbox("対象カテゴリ",categorys)


countrys = ['全て', 'アメリカ', 'イギリス', 'イタリア', 'ウクライナ', 'オーストラリア',
             'カナダ', 'ギリシャ', 'スイス', 'スペイン', 'タイ',
            'デンマーク', 'ドイツ', 'フランス', 'ブラジル', '中国', '台湾', '日本', '韓国',"-"]
country = st.sidebar.radio("原産国",countrys)

##### メインバー
st.title("ブランド分析")
data = data[(data[category] == category_number) & (data['人数'] == top_number)]
if country == '全て':
    pass
else:
    data = data[data['原産国'] == country]

# データをStreamlitアプリケーションに表示
new_col = ['ブランド名', '原産国','人数','レディース', 'メンズ', 'ビューティ',
       'ベビーキッズ', 'ホーム', 'スポーツ',  'URL', '1位', '2位', '3位', '最近レディース', '最近メンズ', '最近ビューティ',
       '最近ベビーキッズ', '最近ホーム', '最近スポーツ']

st.markdown("抽出数:" + str(len(data)))
st.dataframe(data[new_col],hide_index=False)
