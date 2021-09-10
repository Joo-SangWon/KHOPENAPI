import pandas as pd
import os
from pykiwoom.kiwoom import *
import datetime
import time
import supplementary_indi as indi
import pymysql
import pymysql.cursors
import numpy as np
from sqlalchemy import create_engine

# 키움오픈API 객체 생성
kiwoom = Kiwoom()
kiwoom.CommConnect(block=True)

# 현재 디렉토리
flist = os.listdir()
# xlsx 파일로 불러온 거 다 가져오기(현재 디렉토리)
xlsx_list = [x for x in flist if x.endswith('.xlsx')]
# 빈 리스트 생성
close_data = []

# 문자열로 오늘 날짜 시간얻기
current_time = datetime.datetime.now()
now = current_time.strftime("%Y-%m-%d %H:%M:%S")
# DATA가 너무커서 지표로 i 필요
i=0
print("총 데이터 파일의 갯수는 ", len(xlsx_list), "입니다.")

# 파일들 하나씩 불러와서 보조지표 생성해줘야함(제공을 안하니까)
for xls in xlsx_list: # xls가 하나하나의 xlsx 파일 이름들
    code = xls.split('.')[0] # 이거는 파일이름에서 확장자를 .으로 구분하고 앞에 것만 가져온 것
    # print(code) # 종목이름
    df = pd.read_excel(xls)
    # ['칼럼이름'] 하면 하나의 칼럼을 가져올 수 있고
    # [['','']] 이런식이면 2개 이상의 열을 가져온다 []에다가 리스트형식
    필요칼럼 = ['일자', '현재가', '시가', '고가','저가','거래량','거래대금']
    df2 = df[필요칼럼].copy()
    # 종목명 가져와서 붙이기
    code_name = kiwoom.GetMasterCodeName(code)
    # print(code_name)
    # CREATE COLUMNS
    # CREATE PRIMARY_KEY
    df2['ITEM_NM'] = code_name
    df2['ITEM_CD'] = code
    df2['레코드생성일시'] = now

    # 보조지표 생성 by suppplementary_indi
    df2 = indi.cal_MONEY_Ye(df2)
    df2 = indi.cal_FLUCTU_RATE(df2)
    df2 = indi.cal_mv_Av(df2)
    df2 = indi.cal_LINE(df2)
    df2 = indi.cal_GLINE(df2)
    df2 = indi.cal_POST_SPAN(df2)
    df2 = indi.cal_PRE_SPAN_1(df2)
    df2 = indi.cal_PRE_SPAN_2(df2)
    df2 = indi.cal_mv_Av_quant(df2)
    df2 = indi.cal_DISPARITY(df2)
    df2 = indi.cal_PSY_RATE(df2)

    # UPDATE COLUMNS
    df2.rename(columns={'현재가':'FINAL_PRICE', '일자':'DATE',
                        '고가':'HIGH_PRICE', '저가':'LOW_PRICE',
                        '거래량':'TRADING_AMOUNT', '거래대금':'TRADING_MOENY',
                        '시가':'START_PRICE','레코드생성일시':'CREATE_DTTM'}, inplace=True)
    # Redinex COLUMNS in line with SQL Schema
    df2 = df2.reindex(columns=['ITEM_CD', 'DATE', 'ITEM_NM', 'START_PRICE', 'HIGH_PRICE',
                             'LOW_PRICE', 'FINAL_PRICE',
                             'MONEY_YESTERDAY', 'FLUCTU_RATE', 'FP_MAVG_5D', 'FP_MAVG_10D', 'FP_MAVG_20D',
                             'FP_MAVG_60D', 'FP_MAVG_120D', 'FP_MAVG_240D', 'FP_MAVG_1000D',
                             'TP_LINE_9', 'G_LINE_26', 'POST_SPAN_26', 'PRE_SPAN_1', 'PRE_SPAN_2', 'TRADING_AMOUNT',
                             'TA_MAVG_5D', 'TA_MAVG_20D', 'TA_MAVG_60D', 'TA_MAVG_120D', 'TRADING_MONEY',
                             'DISPARITY_5D', 'DISPARITY_10D', 'DISPARITY_20D', 'DISPARITY_60D',
                             'DISPARITY_120D', 'DISPARITY_240D', 'DISPARITY_1000D', 'PSY_RATE_5D',
                             'PSY_RATE_10D', 'PSY_RATE_20D', 'CREATE_DTTM'])

    # 행 인덱싱 df.loc['index 명'(여기가 행),''(열도 같이 가능)]
    # df.loc[:,열] 열 불러오는 것 = df[칼럼명]
    # df.loc[:,[칼럼명,칼럼명]] 가능 = df[[칼럼명, 칼럼명]]
    # df.iloc - numpy의 array 인덱싱 방식
    # 칼럼명이 아니라 index숫자로서 가져올 때 쓴다
    # df.iloc[3:5, 0:2] 4번쨰행부터 5번쨰행, 1번쨰열부터 2번쨰열

    # print(df2)
    # df2 = df2[::-1] # 순서 바꾼 것

    # 보조지표 계산된 table을 list에 이어 붙이기
    close_data.append(df2)
    print("{}개의 데이터가 append 되었습니다.".format(i+1))
    i=i+1

    # 데이터가 너무 커서 1000개씩 insert를 해도 한 번에 sql로 옮길 수가 없음 나눠서 해야함
    if (i+1) % 500 == 0:
        print("현재 merge한 테이블의 갯수는", i+1, "개 입니다.")
        print("현재까지 merge된 테이블들을 DB화 중입니다.")

        # 각각의 list를 df로 이어 붙이기
        df = pd.concat(close_data, ignore_index=True)
        # MySql이 nan을 못 읽어서 none으로 변경해줘야함
        df = df.replace({np.nan: None})
        # print("현재 row의 갯수는", df.shape[0] ,"개 입니다.")

        # MySql DB 연결 (engine의 경우 mysql만 쓰게되면 pymysql을 찾을 수 없다는 module err발생 그래서 + pymysql)
        engine = create_engine("mysql+pymysql://root:0124@localhost/adc")
        # dataframe의 to_sql 이용
        df.to_sql('0600_dp_test', engine, if_exists='append', index=False, chunksize=1000, method='multi')

        print("{}개 종목의 DB화가 끝났습니다.".format(i+1))

        # close_data 초기화를 해야함
        close_data = []

    # 프로그램상에서 merge 해서 캐시저장은 가능
    # 하지만 데이터가 너무 커서 xlsx로는 만들수가 없고 그래서 여기서 파생변수 다 만든 다음에
    # sql로 바로 보내줘야함
    # 여기서 파생변수 다 만들면 그게 0600_dp

# for 문 끝나고 if 문에서 해결 못한 나머지 table들을 마저 insert 해줘야함
# concat
print("현재 남은 테이블의 갯수는 ",len(close_data), "개 입니다.")
df = pd.concat(close_data, ignore_index=True)
df = df.replace({np.nan: None})

# print(df.columns)
# print("현재 칼럼의 갯수는 {0}개 입니다.".format(len(df.columns)))
print("All Data merge finished")
print("남은 Table들을 DB화 중입니다.")

engine = create_engine("mysql+pymysql://root:0124@localhost/adc")
df.to_sql('0600_dp_test', engine, if_exists='append', index=False, chunksize=1000, method='multi')

# 이쪽부터는 to_sql이 아니라 pymysql cursor로 작업하려고 했던 부분
# INSERT가 데이터를 한 번에 옮기는 작업이라 sql문이 길어지고 데이터가 많아지면
# 작동을 안해서 이 코드는 사용을 못 함
# judo_db = pymysql.connect(
#         user='root',
#         password='0124',
#         host='127.0.0.1',
#         db='adc',
#         charset='utf8'
#     )
# cursor = judo_db.cursor(pymysql.cursors.DictCursor)
# sql = '''INSERT INTO 0600_dp_test (ITEM_CD, DATE, ITEM_NM, START_PRICE, HIGH_PRICE,LOW_PRICE, FINAL_PRICE,
#     MONEY_YESTERDAY, FLUCTU_RATE, FP_MAVG_5D, FP_MAVG_10D, FP_MAVG_20D,
#     FP_MAVG_60D, FP_MAVG_120D, FP_MAVG_240D, FP_MAVG_1000D,
#     TP_LINE_9, G_LINE_26, POST_SPAN_26, PRE_SPAN_1, PRE_SPAN_2, TRADING_AMOUNT,
#     TA_MAVG_5D, TA_MAVG_20D, TA_MAVG_60D, TA_MAVG_120D, TRADING_MONEY,
#     DISPARITY_5D, DISPARITY_10D, DISPARITY_20D, DISPARITY_60D,
#     DISPARITY_120D, DISPARITY_240D, DISPARITY_1000D, PSY_RATE_5D,
#     PSY_RATE_10D, PSY_RATE_20D, CREATE_DTTM) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
#     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
# df = listdf.itertuples(index=false, name=none))
# df를 tuple array로 변환하여 executemany param사용가능
# df = df.replace({np.nan: None})
# df = np.array(df)
# df = df.tolist()

# cursor.executemany(sql, df)
# judo_db.commit()

print("데이터 DB화 작업이 끝났습니다.")
print("Data Insert MySql DB finished")

# list에 하나씩 저장되어있던 종목들을 쭉 이어 붙여주는 것
# df.to_excel("merge.xlsx")
# 데이터가 커서 엑셀로 안 만들어짐 바로 sql로 보내야함 - 끊어서 보내야함