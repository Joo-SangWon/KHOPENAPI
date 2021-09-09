import pandas as pd
import os
from pykiwoom.kiwoom import *
import datetime
import time
import supplementary_indi as indi

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
i=0
print("총 데이터 파일의 갯수는 ", len(xlsx_list), "입니다.")

for xls in xlsx_list: # xls가 하나하나의 xlsx 파일 이름들
    # if i == 1:
    #     break
    code = xls.split('.')[0] # 이거는 파일이름에서 확장자를 .으로 구분하고 앞에 것만 가져온 것
    # print(code) # 종목이름
    df = pd.read_excel(xls)
    # ['칼럼이름'] 하면 하나의 칼럼을 가져올 수 있고
    # [['','']] 이런식이면 2개 이상의 열을 가져온다 []에다가 리스트형식
    필요칼럼 = ['일자', '현재가', '시가', '고가','저가','거래량','거래대금']
    df2 = df[필요칼럼].copy()
    # 종목이름 가져와서 붙이기
    code_name = kiwoom.GetMasterCodeName(code)
    # print(code_name)
    df2['ITEM_NM'] = code_name
    df2['ITEM_CD'] = code
    df2['레코드생성일시'] = now
    # 중간 데이터 확인
    # print(len(df2)) # 600
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

    # df2 = indi.cal_MONEY_Ye(df2)
    df2.rename(columns={'현재가':'FINAL_PRICE', '일자':'DATE',
                        '고가':'HIGH_PRICE', '저가':'LOW_PRICE',
                        '거래량':'TRADING_AMOUNT', '거래대금':'TRADING_MOENY',
                        '시가':'START_PRICE','레코드생성일시':'CREATE_DTTM'}, inplace=True)
    # df2 = df2.set_index('일자')
    # df[0:3] 3행까지 불러옴
    # 행 인덱싱 df.loc['index 명'(여기가 행),''(열도 같이 가능)]
    # df.loc[:,열] 열 불러오는 것 = df[칼럼명]
    # df.loc[:,[칼럼명,칼럼명]] 가능 = df[[칼럼명, 칼럼명]]
    # df.iloc - numpy의 array 인덱싱 방식
    # 칼럼명이 아니라 index숫자로서 가져올 때 쓴다
    # df.iloc[3:5, 0:2] 4번쨰행부터 5번쨰행, 1번쨰열부터 2번쨰열

    # print(df2)
    # df2 = df2[::-1] # 순서 바꾼 것
    # print(df2)
    # break
    # 이코드가 무슨 코드인지 이해가 안됨
    close_data.append(df2)
    print("{}개의 데이터가 append 되었습니다.".format(i))
    # 이렇게 되면 0600_dp 기본 테이블 구조 생성?
    i=i+1

    # 프로그램상에서 merge 해서 캐시저장은 가능
    # 하지만 데이터가 너무 커서 xlsx로는 만들수가 없고 그래서 여기서 파생변수 다 만든 다음에
    # sql로 바로 보내줘야함
    # 여기서 파생변수 다 만들면 그게 0600_dp

# concat
print("현재 병합한 테이블의 갯수는 ",len(close_data), "개 입니다.")
df = pd.concat(close_data)
print(df.columns)
print("현재 칼럼의 갯수는 {0}개 입니다.".format(len(df.columns)))
print("현재 row의 갯수는", df.shape[0] ,"개 입니다.")
# df2 = pd.concat(close_data, axis=1)
# print(len(df2.columns))
# print(df.head(7))
# print(df.tail(7))
print("Data merge finished")

# list에 하나씩 저장되어있던 종목들을 쭉 이어 붙여주는 것
# df.to_excel("merge.xlsx")
# 데이터가 커서 엑셀로 안 만들어짐 바로 sql로 보내야함