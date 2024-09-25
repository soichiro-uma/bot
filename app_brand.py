import streamlit as st
import boto3
import pandas as pd
from io import StringIO
import datetime
from boto3.dynamodb.conditions import Key

# AWS DynamoDBクライアントを作成
dynamodb = boto3.resource('dynamodb',
                           region_name=st.secrets["c"],
                           aws_access_key_id= st.secrets["a"],
                           aws_secret_access_key= st.secrets["b"],)
# アクセスするテーブル名
table_name = st.secrets["db_dynamo"]
table = dynamodb.Table(table_name)

# クエリ条件などがある場合（例: プライマリキーで検索）
def get_data_from_dynamodb(i):
    try:
        # データの取得（例: プライマリキー "id" の値が "1" のアイテムを取得）
        response = table.query(
            KeyConditionExpression=Key('No').eq(i)
        )
        ck = response['Items'][0]["Status_now"]
        if ck == "Active":
            items = response['Items'][0]["Mail_address"]
        return items
    except Exception as e:
        # st.error(f"Error fetching data: {e}")
        return "---not---"
    
# DynamoDBからデータを取得して表示
data = []
for i in range(0,100):
    data.append(get_data_from_dynamodb(i))
st.write(data)
### ユーザ判定
mail = st.sidebar.text_input("メールアドレスを入れてください")
if mail in data:
    ##### データ読み込み
    # S3のバケット名とCSVファイルのキー（パス）を指定
    bucket_name = st.secrets["db_backet"]

    # S3にアクセスするためのセッションを作成
    s3_client = boto3.client('s3',
        aws_access_key_id= st.secrets["a"],
        aws_secret_access_key= st.secrets["b"],
        region_name=st.secrets["c"])

    # 現在の日時を取得
    now = datetime.datetime.now()- datetime.timedelta(days=3)
    formatted_now = now.strftime("%Y%m%d")
    csv_file_key = f"{formatted_now}_all.csv"
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
elif mail == "":
    pass
else:
    st.write('ツールのユーザ登録がされていません')