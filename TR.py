from pykiwoom.kiwoom import *
import time

kiwoom = Kiwoom()
kiwoom.CommConnect(block=True)

# 전종목 일봉데이터를 엑셀로 저장하기
# 전종목 종목코드
kospi = kiwoom.GetCodeListByMarket('0')
kosdaq = kiwoom.GetCodeListByMarket('10')
codes = kospi + kosdaq

# 문장열로 오늘 날짜 얻기
now = datetime.datetime.now()
today = now.strftime('%Y%m%d')

num = 1
# 전 종목의 일봉 데이터
# enumerate 몇 번째 반복문인지 확인하고 싶을때 튜플형태로 반환
# 그래서 i로 몇 번째 인지 알 수 있음(index, list의 element) 형태
# TR 요청 안했었음 한 번에 600개 행만 보내줌
# 장시작까지는 8700개의 행이 필요함
for i, code, in enumerate(codes):
    if i < 627:
        continue # 20210903 13:04 441까지하다가 lock걸림
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
        # 시간 더 늘렸음 - 296번째에서 과도한 traffic 발생한다고 함
        time.sleep(2.5)

    print("TR 추가 요청이 끝났습니다.")
    df = pd.concat(dfs)
    num = 1
    out_name = f"{code}.xlsx"

    df.to_excel(out_name)
    time.sleep(4.5)

print("ALL TR finished")