from pykiwoom.kiwoom import *
import pprint

# 로그인
kiwoom = Kiwoom()
kiwoom.CommConnect(block = True)
# 블록킹 로그인 > 로그인이 완료될 때 까지 다음 줄의 코드가 실행되지 않음
print("블록킹 로그인 완료")

# 사용자 정보 얻어오기 by GetLogininfo
account_num = kiwoom.GetLoginInfo("ACCOUNT_CNT") # 전체 계좌수
accounts = kiwoom.GetLoginInfo("ACCNO") # 전체 계좌 리스트
user_id = kiwoom.GetLoginInfo("USER_ID") # 사용자 ID
user_name = kiwoom.GetLoginInfo("USER_NAME") # 사용자 이름
keyboard = kiwoom.GetLoginInfo("KEY_BSECGB") # 키보드 보안 해지여부
firewall = kiwoom.GetLoginInfo("FIREW_SECGB") # 방화벽 설정 여우

# print(account_num)
# print(accounts)
# print(user_id)
# print(user_name)
# print(keyboard)
# print(firewall)

# 종목 코드 얻기 0 코스피 10 코스닥
kospi = kiwoom.GetCodeListByMarket("0")
kosdaq = kiwoom.GetCodeListByMarket("10")

# print(len(kospi), kospi)
# print(len(kosdaq), kosdaq)

# 종목명 얻기
name = kiwoom.GetMasterCodeName("005930")
print(name)

# 서버 연결상태 확인
state = kiwoom.GetConnectState()
if state == 0:
    print("미연결")
elif state == 1:
    print("연결완료")

# 상장주식수 얻기
stock_cnt = kiwoom.GetMasterListedStockCnt("005930")
print("삼성전자 상장주식수 : ", stock_cnt)
# 이 부분은 버그로 인해서 상장주식수가 21억까지밖에 표현인 안됨

# 감리구분
# 정상, 투자주의, 투자경고, 투자위험, 투자주의환기종목
감리구분 = kiwoom.GetMasterConstruction("005930")
print(감리구분)

# 전일가
# 전일 종가 리턴함
전일가 = kiwoom.GetMasterLastPrice("005930")
print(int(전일가))

# 종목상태
종목상태 = kiwoom.GetMasterStockState("005930")
print(종목상태)

# 테마
# 테마 그룹
group = kiwoom.GetThemeGroupList(1)
pprint.pprint(group)

# 테마별 종목코드
tickers = kiwoom.GetThemeGroupCode("330")
# 화장품 테마에 속하는 종목 코드 리스트
# print(tickers)
for ticker in tickers:
    name = kiwoom.GetMasterCodeName(ticker)
    print(ticker, name)

# 조건검색
# 조건식을 pc로 다운로드
# kiwoom.GetConditionLoad()
# 전체 조건식 리스트 얻기
# conditions = kiwoom.GetConditionNameList()
# # 0번 조건식에 해당하는 종목 리스트 출력
# condition_index = conditions[0][0]
# condition_name = conditions[0][1]
# codes = kiwoom.SendCondition("0101", condition_name, condition_index, 0)
# # sendcodition parm 화면번호, 조건명, 조건명인덱스, 조회구분(0 일반조회, 1 실시간 조회, 2 연속조회)
# # OnReceiveTrCondtion 으로 결과값이 옴
# print(codes)

# 주문 by SendOrder
# parm name 사용자 정의 명, 화면번호, 계좌번호, 주문유형, 주식종목코드, 주문수량, 주문단가, 거래구분, 원주문번호
# 매수
# 주식계좌
stock_account = accounts[0]
# 삼성전자 10주 시장가 매수
kiwoom.SendOrder("시장가매수", "0101", stock_account, 1, "005930", 10, 0, "03","")
# 매도
# 삼성전자 10주 시장가 매도
kiwoom.SendOrder("시장가매도", "0101", stock_account, 2, "005930", 10, 0, "03","")

