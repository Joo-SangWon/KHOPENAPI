import pandas as pd

# 필요칼럼 = ['일자', '현재가'(종가), '시가', '고가','저가','거래량','거래대금']

# 추가해야할 보조지표(파생변수)

# 전일대비 등락가격
# 당일종가 - 전일종가
def cal_MONEY_Ye(df):
    df['MONEY_YESTERDAY'] = df['현재가'] - df['현재가'].shift(1)
    return df

# # 전일대비 등락비율
# # 전일대비등락가격 / 전일종가 * 100
def cal_FLUCTU_RATE(df):
    df['FLUCTU'] = df['MONEY_YESTERDAY'] / df['현재가'].shift(1) * 100
    return df

# 이동평균
# 5, 10, 20, 60, 120, 240, 1000
# 다 이전 일수 들로만 계산
def cal_mv_Av(df):
    df['FP_MAVG_5D'] = df['현재가'].rolling(5).mean()
    df['FP_MAVG_10D'] = df['현재가'].rolling(10).mean()
    df['FP_MAVG_20D'] = df['현재가'].rolling(20).mean()
    df['FP_MAVG_60D'] = df['현재가'].rolling(60).mean()
    df['FP_MAVG_120D'] = df['현재가'].rolling(120).mean()
    df['FP_MAVG_240D'] = df['현재가'].rolling(240).mean()
    df['FP_MAVG_1000D'] = df['현재가'].rolling(1000).mean()
    return df

# 전환선
# (최근 9일간 최고가격 + 최근 9일간 최저가격) / 2
def cal_LINE(df):
    df['TP_LINE_9'] = (df['현재가'].rolling(9).max() + df['현재가'].rolling(9).min()) / 2
    return df

# 기준선
# (최근 26일간 최고가격 + 최근 26일간 최저가격) / 2
def cal_GLINE(df):
    df['G_LINE_26'] = (df['현재가'].rolling(26).max() + df['현재가'].rolling(26).min()) / 2
    return df

# 후행스팬
# 금일 종가를 26일전에 기입
def cal_POST_SPAN(df):
    df['POST_SPAN_26'] = df['현재가'].shift(-26)
    return df

# 선행스팬1
# (금일 전환선값 + 금일 기준선 값) / 2, 이 수치를 26일 후에 기입
def cal_PRE_SPAN_1(df):
    df['PRE_SPAN_1'] = ((df['TP_LINE_9'] + df['G_LINE_26']) / 2).shift(26)
    return df
# 선행스팬2
# (과거52일간의 최고가 + 과거 52일간의 최저가) / 2, 이 수치를 26일 후에 기입
def cal_PRE_SPAN_2(df):
    print(type(df['현재가']))
    df['PRE_SPAN_2'] = ((df['현재가'].rolling(52).max() + df['현재가'].rolling(52).min()) / 2).shift(26)
    return df

# 거래량 이동평균
# 다 이전 일수 들로만 계산
def cal_mv_Av_quant(df):
    df['TA_MAVG_5D'] = df['거래량'].rolling(5).mean()
    df['TA_MAVG_20D'] = df['거래량'].rolling(20).mean()
    df['TA_MAVG_60D'] = df['거래량'].rolling(60).mean()
    df['TA_MAVG_120D'] = df['거래량'].rolling(120).mean()
    return df

# 이격도
# 주가랑 이동평균선간의 괴리 정도
# 주가 / 이동평균 * 100
def cal_DISPARITY(df):
    df['DISPARITY_5D'] = (df['현재가'] / df['FP_MAVG_5D']) * 100
    df['DISPARITY_10D'] = (df['현재가'] / df['FP_MAVG_10D']) * 100
    df['DISPARITY_20D'] = (df['현재가'] / df['FP_MAVG_20D']) * 100
    df['DISPARITY_60D'] = (df['현재가'] / df['FP_MAVG_60D']) * 100
    df['DISPARITY_120D'] = (df['현재가'] / df['FP_MAVG_120D']) * 100
    df['DISPARITY_240D'] = (df['현재가'] / df['FP_MAVG_240D']) * 100
    df['DISPARITY_1000D'] = (df['현재가'] / df['FP_MAVG_1000D']) * 100
    return df
# 100퍼 이상이면 당일 주가가 이동평균선보다 위
# 100퍼 이하면 당일 주가가 이동평균선보다 아래

# 심리도
# 이전 거래일들 중에 상승한 날들을 포함
def cnt(x):
    prev_count = 0
    for i in x:
        if i > 0:
            prev_count += 1
    return prev_count

def cal_PSY_RATE(df):
    df['PSY_RATE_5D'] = df['MONEY_YESTERDAY'].rolling(5).apply(cnt) / 5 * 100
    df['PSY_RATE_10D'] = df['MONEY_YESTERDAY'].rolling(10).apply(cnt) / 10 * 100
    df['PSY_RATE_20D'] = df['MONEY_YESTERDAY'].rolling(20).apply(cnt) / 20 * 100
    return df