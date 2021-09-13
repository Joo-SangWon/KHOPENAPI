# 키움오픈 API로 DB화가 끝났고 이제는
# 매일 새로운 데이터를 받아서 최신화 하는 코드가 필요함
# TR요청 중에 전 종목의 장마감 하루치 데이터가 없어서
# 결국에 주식일봉차트 조회후 최신날짜만 골라서 insert해야함
# 따라서 table 내 특정 종목 데이터이 가장 최근 insert날짜를 반환
# by CREATE_DTTM
# NEW 전략 - TR 이 600일치 data 밖에 안주는데 차라리 2개불러서 파생변수 계산후에

import numpy as np
import pymysql
from sqlalchemy import create_engine
from pykiwoom.kiwoom import *
import datetime
import supplementary_indi as indi
from datetime import timedelta

# KHOPENAPI 객체 생성
kiwoom = Kiwoom()
kiwoom.CommConnect(block=True)

# 전종목 장마감 일봉데이터를 가져와야함
kospi = kiwoom.GetCodeListByMarket('0')
kosdaq = kiwoom.GetCodeListByMarket('10')
codes = kospi + kosdaq

# 문장열로 오늘 날짜 시간 얻기
now = datetime.datetime.now()
# today는 request의 input param 으로 쓰기 위해
today = now.strftime('%Y%m%d')
# now_time은 시간까지 합쳐서 CREATE_DTTM column 작성을 위해
now_time = now.strftime("%Y-%m-%d %H:%M:%S")

# 이쪽부터는 to_sql이 아니라 pymysql cursor로 작업하려고 했던 부분
# INSERT가 데이터를 한 번에 옮기는 작업이라 sql문이 길어지고 데이터가 많아지면
# 작동을 안해서 이 코드는 사용을 못 함
conn = pymysql.connect(
        user='root',
        password='0124',
        host='127.0.0.1',
        db='adc',
        charset='utf8'
    )
cursor = conn.cursor(pymysql.cursors.DictCursor)

# cursor.executemany(sql, df)
# judo_db.commit()
# 이 부분 필요 없을 듯
# sql = 'select max(CREATE_DTTM) from 0600_dp_test'
# cursor.execute(sql)
# result = cursor.fetchall()
# result = pd.DataFrame(result)
# result 결과값이 timestamp 로 받아지는데 바로 timestamp에서
# datetime 형태로 갈 수가 없음 그래서 >str>datetiem 후 비교
# s = str(result.iloc[0,0])
# latest_date = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
#
# print(latest_date)
# testday = (latest_date.today() - timedelta(10)).strftime('%Y%m%d')
# print(testday)
# print(codes[0])

# 전 종목의 일봉 데이터
for i, code, in enumerate(codes):
    dfs=[]
    print(f"{i}/{len(codes)} {code}")
    df = kiwoom.block_request("opt10081",
                              종목코드=code,
                              기준일자=today,
                              수정주가구분=1,
                              output="주식일봉차트조회",
                              next=0)
    dfs.append(df)

    while kiwoom.tr_remained:
        print("{}th 종목의 Data가 남아있어 추가 TR 요청을 {} 번째 중입니다.".format(i, num))
        df = kiwoom.block_request("opt10081",
                                  종목코드=code,
                                  기준일자=today,
                                  수정주가구분=1,
                                  output="주식일봉차트조회",
                                  next=2)
        dfs.append(df)
        num += 1
        time.sleep(1.5)
        if num == 2:
            break

    print("TR 추가 요청이 끝났습니다.")
    # 그러면 지금 여기에 1200개 행의 데이터가 있음 충분히 모든 지수 계산 가능
    df = pd.concat(dfs)
    num = 1
    # 불러와서 데이터 계산후 새로 삽입까지 여기 py 파일에서 하는거로

    # indicator 만드는 부분
    # 쓸모없는 columns 날리기
    필요칼럼 = ['일자', '현재가', '시가', '고가', '저가', '거래량', '거래대금']
    df2 = df[필요칼럼].copy()

    code_name = kiwoom.GetMasterCodeName(code)
    df['ITEM_NM'] = code_name
    df['ITEM_CD'] = code
    df['레코드생성일시'] = now

    # 보조지표 생성 by suppplementary_indi
    df = indi.cal_MONEY_Ye(df)
    df = indi.cal_FLUCTU_RATE(df)
    df = indi.cal_mv_Av(df)
    df = indi.cal_LINE(df)
    df = indi.cal_GLINE(df)
    df = indi.cal_POST_SPAN(df)
    df = indi.cal_PRE_SPAN_1(df)
    df = indi.cal_PRE_SPAN_2(df)
    df = indi.cal_mv_Av_quant(df)
    df = indi.cal_DISPARITY(df)
    df = indi.cal_PSY_RATE(df)

    # UPDATE COLUMNS
    df.rename(columns={'현재가': 'FINAL_PRICE', '일자': 'DATE',
                        '고가': 'HIGH_PRICE', '저가': 'LOW_PRICE',
                        '거래량': 'TRADING_AMOUNT', '거래대금': 'TRADING_MOENY',
                        '시가': 'START_PRICE', '레코드생성일시': 'CREATE_DTTM'}, inplace=True)
    # Redinex COLUMNS in line with SQL Schema
    df = df.reindex(columns=['ITEM_CD', 'DATE', 'ITEM_NM', 'START_PRICE', 'HIGH_PRICE',
                               'LOW_PRICE', 'FINAL_PRICE',
                               'MONEY_YESTERDAY', 'FLUCTU_RATE', 'FP_MAVG_5D', 'FP_MAVG_10D', 'FP_MAVG_20D',
                               'FP_MAVG_60D', 'FP_MAVG_120D', 'FP_MAVG_240D', 'FP_MAVG_1000D',
                               'TP_LINE_9', 'G_LINE_26', 'POST_SPAN_26', 'PRE_SPAN_1', 'PRE_SPAN_2', 'TRADING_AMOUNT',
                               'TA_MAVG_5D', 'TA_MAVG_20D', 'TA_MAVG_60D', 'TA_MAVG_120D', 'TRADING_MONEY',
                               'DISPARITY_5D', 'DISPARITY_10D', 'DISPARITY_20D', 'DISPARITY_60D',
                               'DISPARITY_120D', 'DISPARITY_240D', 'DISPARITY_1000D', 'PSY_RATE_5D',
                               'PSY_RATE_10D', 'PSY_RATE_20D', 'CREATE_DTTM'])

    # indicator 추가 끝


    # sql로 밀어넣어야 함
    # nan > none 으로 치환
    df = df.replace({np.nan: None})
    # 이거는 insert랑 update 써야 하니까 to_sql 말고 pymysql 써야할 듯
    judo_db = pymysql.connect(
            user='root',
            password='0124',
            host='127.0.0.1',
            db='adc',
            charset='utf8'
        )
    cursor = judo_db.cursor(pymysql.cursors.DictCursor)
    # 이거 수정해야함
    sql = '''INSERT INTO 0600_dp_test (ITEM_CD, DATE, ITEM_NM, START_PRICE, HIGH_PRICE,LOW_PRICE, FINAL_PRICE,
        MONEY_YESTERDAY, FLUCTU_RATE, FP_MAVG_5D, FP_MAVG_10D, FP_MAVG_20D,
        FP_MAVG_60D, FP_MAVG_120D, FP_MAVG_240D, FP_MAVG_1000D,
        TP_LINE_9, G_LINE_26, POST_SPAN_26, PRE_SPAN_1, PRE_SPAN_2, TRADING_AMOUNT,
        TA_MAVG_5D, TA_MAVG_20D, TA_MAVG_60D, TA_MAVG_120D, TRADING_MONEY,
        DISPARITY_5D, DISPARITY_10D, DISPARITY_20D, DISPARITY_60D,
        DISPARITY_120D, DISPARITY_240D, DISPARITY_1000D, PSY_RATE_5D,
        PSY_RATE_10D, PSY_RATE_20D, CREATE_DTTM) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
    # df = listdf.itertuples(index=false, name=none))
    # df를 tuple array로 변환하여 executemany param사용가능
    df = df.replace({np.nan: None})
    df = np.array(df)
    df = df.tolist()
    # df[0] - 1st 행
    # 바뀌는 값들이 파생변수 다 바뀌네
    # 기준day 가 5/10/20/60/120/240/1000
    # 이거는 새로운 날의 주식 데이터 Input
    cursor.execute(sql, df[0])

    # 바뀐 데이터 수정
    # 바꿀 날짜들 생성
    print(map(lambda day: now - timedelta(day), [5, 10, 20, 60, 120, 240, 1000]))
    day_list = list(map(lambda day: (now - timedelta(day)).date(), [5, 10, 20, 26, 60, 120, 240, 1000]))
    day_list_str = list(map(lambda day: (now - timedelta(day)).date().strftime("%Y-%m-%d"),
                            [5, 10, 20, 26, 60, 120, 240, 1000]))

    sql = '''update 0600_dp_test set  FP_MAVG_5D = %s, TA_MAVG_5D =%s, 
            DISPARITY_5D = %S where date = {}
    '''.format(day_list_str[0])
    cursor.execute(sql, df[5])
    # 이렇게 하면 5일전의 data까지 업데디트 완료

    # 여기서 df는 수정한거로
    cursor.executemany(sql, df)




    judo_db.commit()


    time.sleep(4)

nowtime = time.strftime("%H%M", time.localtime(time.time()))


def latest_date():
    latest_date = []
    # 4시 이전인지 이후인지 판단하기
    print(nowtime)
    if nowtime < "1600":
        print("4시 이전입니다")
        current_date = (now - datetime.timedelta(days=1)).date()
        for i in range(7):
            variable = current_date - datetime.timedelta(days=i)
            print(variable)
            justify_week = variable.weekday()
            print(justify_week)
            if justify_week > 4:
                pass
            else:
                latest_date.append(variable)

        return latest_date[:1]
    else:
        for i in range(7):
            variable = current_date - datetime.timedelta(days=i)
            jusify_week = current_date.weekday()
            if justify_week > 4:
                pass
            else:
                latest_date.append(variable)

        return latest_date[:1]