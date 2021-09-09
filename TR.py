from pykiwoom.kiwoom import *
import time
import pandas as pd

kiwoom = Kiwoom()
kiwoom.CommConnect(block=True)

# 싱글데이터 TR
# df = kiwoom.block_request("opt10001",
#                           종목코드 = "005930",
#                           output="주식기본정보",
#                           next=0) # next=0 단일정보조회시
# print(df)

# 멀티데이터 TR
# 주식일봉차트조회
# TR요청 (연속조회)
# dfs = []
# df = kiwoom.block_request("opt10081",
#                           종목코드 = "005930",
#                           기준일자 = "20210907",
#                           수정주가구분 = 1,
#                           output="주식일봉차트조회",
#                           next=0)
# print(df.head())
# dfs.append(df)
#
# while kiwoom.tr_remained:
#     df = kiwoom.block_request("opt10081",
#                               종목코드 = "005930",
#                               기준일자 = "20210907",
#                               수정주가구분 = 1,
#                               output = "주식일봉차트조회",
#                               next=2)
#     dfs.append(df)
#     time.sleep(1)
#
# df= pd.concat(dfs)
# df.to_excel("005930.xlsx")

# 전종목 일봉데이터를 엑셀로 저장하기
# 전종목 종목코드
kospi = kiwoom.GetCodeListByMarket('0')
kosdaq = kiwoom.GetCodeListByMarket('10')
codes = kospi + kosdaq

# 문장열로 오늘 날짜 얻기
now = datetime.datetime.now()
today = now.strftime('%Y%m%d')

# 전 종목의 일봉 데이터
# enumerate 몇 번째 반복문인지 확인하고 싶을때 튜플형태로 반환
# 그래서 i로 몇 번째 인지 알 수 있음(index, list의 element) 형태
for i, code, in enumerate(codes):
    print(f"{i}/{len(codes)} {code}")
    df = kiwoom.block_request("opt10081",
                              종목코드=code,
                              기준일자=today,
                              수정주가구분=1,
                              output="주식일봉차트조회",
                              next=0)
    out_name = f"{code}.xlsx"
    df.to_excel(out_name)
    time.sleep(3.6)