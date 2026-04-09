import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import base64
import requests
from openpyxl.utils import column_index_from_string
import os
import io
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# 1. 페이지 설정 (넓은 화면 모드)
st.set_page_config(
    page_title="위드인천에너지 일일 운영 현황",
    layout="wide"
)

# --- 이미지 파일을 Base64로 인코딩하는 함수 ---
@st.cache_data
def get_image_as_base64(path):
    try:
        with open(path, "rb") as img_file:
            return base64.b64encode(img_file.read()).decode()
    except FileNotFoundError:
        st.warning(f"CI 로고 파일({path})을 찾을 수 없습니다. 스크립트와 같은 폴더에 파일이 있는지 확인해주세요.")
        return None

# --- 어제 날짜 생성 (한국 시간 기준) ---
kst = ZoneInfo('Asia/Seoul')
yesterday = datetime.now(kst) - timedelta(days=1)

# --- 2. 로고, 헤더, 날짜 선택 ---
logo_base64 = get_image_as_base64("ci_logo.png")
if logo_base64:
    logo_html = f"<img src='data:image/png;base64,{logo_base64}' style='height: 40px; margin-right: 15px;'>"
else:
    logo_html = "<span style='font-size: 40px; margin-right: 15px;'>📊</span>" # 로고 없을 시 대체 아이콘

header_cols = st.columns([0.5, 0.5])
with header_cols[0]:
    st.markdown(
        f"""
        <div style="display: flex; align-items: center; height: 100%;">
            {logo_html}
            <h1 style='margin: 0; font-weight: 800; font-size: 2.2rem;'>
                위드인천에너지 운영 현황
            </h1>
        </div>
        """,
        unsafe_allow_html=True
    )

with header_cols[1]:
    pass # 상단 우측 공간

# 3. 구분선
st.markdown("<hr style='border: 1px solid #d3d3d3; margin-top: 10px; margin-bottom: 20px;'>", unsafe_allow_html=True)

# --- 전역 조회 기준 UI (우측 상단 배치) ---
current_year = datetime.now(kst).year
years = list(range(2025, current_year + 1))

top_left_col, top_right_col = st.columns([0.6, 0.4])

with top_left_col:
    selected_tab = st.segmented_control(
        "조회 메뉴",
        ["실시간 모니터링", "기간별 운영 현황"],
        default="실시간 모니터링",
        selection_mode="single",
        label_visibility="collapsed",
        key="main_tabs"
    )
    if not selected_tab:
        selected_tab = "실시간 모니터링"

    # 기간별 운영 현황 선택 시에만 서브 탭 표시
    if selected_tab == "기간별 운영 현황":
        selected_sub_tab = st.segmented_control(
            "시설 현황 메뉴",
            ["📈 대기배출시설 현황", "💧 폐수배출시설 현황", "♨️ 외부수열 현황"],
            default="📈 대기배출시설 현황",
            selection_mode="single",
            label_visibility="collapsed",
            key="sub_tabs"
        )
        if not selected_sub_tab:
            selected_sub_tab = "📈 대기배출시설 현황"

with top_right_col:
    if selected_tab == "기간별 운영 현황":
        with st.container(border=True):
            view_type = st.radio(
                "조회 기준",
                ['일별', '월별', '연별'],
                horizontal=True,
                key='view_type'
            )

            if view_type == '일별':
                selected_date = st.date_input(
                    "조회 날짜",
                    yesterday,
                    min_value=datetime(2025, 1, 1),
                    max_value=yesterday,
                    format="YYYY.MM.DD",
                    key="daily_date_picker"
                )
                global_params = {'view_type': '일별', 'date_obj': selected_date}
                period_string = selected_date.strftime('%Y.%m.%d')

            elif view_type == '월별':
                year_col, month_col = st.columns(2)
                with year_col:
                    selected_year = st.selectbox("연도", options=years, index=len(years) - 1, key="monthly_year")
                with month_col:
                    months_in_year = 12 if selected_year != current_year else yesterday.month
                    selected_month = st.selectbox("월", options=range(1, months_in_year + 1), index=months_in_year - 1, key="monthly_month")
                global_params = {'view_type': '월별', 'year': selected_year, 'month': selected_month}
                period_string = f"{selected_year}년 {selected_month}월"

            else: # 연별
                selected_year = st.selectbox("연도", options=years, index=len(years) - 1, key="yearly_year")
                global_params = {'view_type': '연별', 'year': selected_year}
                period_string = f"{selected_year}년"


# --- TMS API 호출 함수 ---
@st.cache_data(ttl=300) # 5분마다 데이터 새로고침
def get_chp_nox_from_api(search_date):
    """
    공공데이터포털(CleanSYS) API를 호출하여 위드인천에너지의 최신 NOx 농도를 가져옵니다.
    """
    # 1. API 요청 정보 설정
    service_key = "88fbd212b04f2b9e7f9e678d1fc903496f49f27c7af7d562dace83486c3effef"
    url = "http://apis.data.go.kr/B552584/cleansys/rltmMesureResult"
    params = {
        'serviceKey': service_key,
        'returnType': 'json',
        'numOfRows': '100',
        'pageNo': '1',
        'searchDate': search_date.strftime('%Y-%m-%d'),
        'wdrCd': '150171', # 위드인천에너지 사업장코드
        'area_nm': '인천광역시' # 조회 지역을 인천광역시로 명확히 지정
    }

    try:
        # 2. API 호출
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # 3. 데이터 파싱 (사용자가 제공한 JSON 구조 기반)
        items = data.get('response', {}).get('body', {}).get('items', [])

        # API가 단일 항목을 dict로 반환하는 경우를 대비해 리스트로 감싸줍니다.
        if isinstance(items, dict):
            items = [items]

        if not items:
            return {'value': '데이터 없음', 'time': None}

        # 첫 번째 데이터에서 사업장 이름과 지역을 확인하여, 올바른 데이터를 가져왔는지 검증합니다.
        first_item = items[0]
        facility_name = first_item.get('fact_manage_nm', '')
        area_name = first_item.get('area_nm', '')

        if '위드인천에너지' not in facility_name:
            error_message = f"잘못된 사업장 코드(150171)입니다. '{facility_name}'의 데이터를 가져왔습니다."
            return {'value': error_message, 'time': None}
        if '인천' not in area_name:
            error_message = f"잘못된 지역의 데이터가 수신되었습니다. (수신된 지역: {area_name})"
            return {'value': error_message, 'time': None}

        # 4. 원하는 데이터 추출 (사용자 제공 필드명 기반)
        for item in reversed(items): # 최신 데이터를 먼저 찾기 위해 역순으로 탐색
            if item.get('stack_code') == '1':
                nox_value = item.get('nox_mesure_value')
                measure_time = item.get('mesure_dt')

                if nox_value is not None:
                    try:
                        formatted_value = f"{float(nox_value):.2f}"
                    except (ValueError, TypeError):
                        formatted_value = nox_value
                    
                    return {'value': formatted_value, 'time': measure_time}
        
        return {'value': 'NOx 값 없음', 'time': None}

    except requests.exceptions.RequestException:
        return {'value': 'API 호출 실패', 'time': None}
    except Exception:
        return {'value': '데이터 처리 오류', 'time': None}

# --- [테스트용] 위드인천에너지 API 호출 함수 ---
@st.cache_data(ttl=300) # 5분마다 데이터 새로고침
def get_with_incheon_energy_test_nox_from_api(search_date):
    """
    [테스트용] 공공데이터포털(CleanSYS) API를 호출하여 위드인천에너지의 최신 NOx 값을 가져옵니다.
    """
    service_key = "88fbd212b04f2b9e7f9e678d1fc903496f49f27c7af7d562dace83486c3effef"
    url = "http://apis.data.go.kr/B552584/cleansys/rltmMesureResult"
    params = {
        'serviceKey': service_key,
        'returnType': 'json',
        'numOfRows': '100',
        'pageNo': '1',
        # searchDate 파라미터를 제거하여 항상 최신 실시간 데이터를 조회합니다.
        'areaNm': '인천',
        'factManageNm': '위드인천에너지',
        'stackCode': '1'
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        items = data.get('response', {}).get('body', {}).get('items', [])
        if isinstance(items, dict):
            items = [items]

        if not items:
            return {'value': '데이터 없음', 'time': None}

        # 첫 번째 데이터에서 사업장 이름을 확인하여, 올바른 데이터를 가져왔는지 검증합니다.
        first_item = items[0]
        facility_name = first_item.get('fact_manage_nm', '')
        if '위드인천에너지' not in facility_name:
            error_message = f"잘못된 사업장 코드(150171)입니다. '{facility_name}'의 데이터를 가져왔습니다."
            return {'value': error_message, 'time': None}

        # 배출구 1번에 대한 데이터 찾기
        for item in reversed(items):
            if item.get('stack_code') == '1':
                nox_value = item.get('nox_mesure_value')
                measure_time = item.get('mesure_dt')
                if nox_value is not None:
                    try:
                        formatted_value = f"{float(nox_value):.2f}"
                    except (ValueError, TypeError):
                        formatted_value = nox_value
                    return {'value': formatted_value, 'time': measure_time}
        
        return {'value': 'NOx 값 없음', 'time': None}

    except Exception as e:
        return {'value': '오류', 'time': None}

# --- [신규] 인천종합에너지 API 호출 함수 ---
@st.cache_data(ttl=300) # 5분마다 데이터 새로고침
def get_incheon_total_energy_nox_from_api():
    """
    [신규] 공공데이터포털(CleanSYS) API를 호출하여 인천종합에너지의 최신 NOx 값을 가져옵니다.
    """
    service_key = "88fbd212b04f2b9e7f9e678d1fc903496f49f27c7af7d562dace83486c3effef"
    url = "http://apis.data.go.kr/B552584/cleansys/rltmMesureResult"
    params = {
        'serviceKey': service_key,
        'returnType': 'json',
        'numOfRows': '100',
        'pageNo': '1',
        'areaNm': '인천',
        'factManageNm': '인천종합에너지',
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        items = data.get('response', {}).get('body', {}).get('items', [])
        if isinstance(items, dict):
            items = [items]

        if not items:
            return {'error': 'API에서 데이터를 수신하지 못했습니다 (items 없음).'}

        # 사업장 이름 확인
        first_item = items[0]
        facility_name = first_item.get('fact_manage_nm', '')
        if '인천종합에너지' not in facility_name:
            return {'error': f"잘못된 사업장명입니다. '{facility_name}'의 데이터를 가져왔습니다."}

        # 배출구별로 최신 데이터 정리
        latest_data_per_stack = {}
        for item in items:
            stack_code = item.get('stack_code')
            if stack_code:
                # 이미 저장된 데이터보다 최신 데이터일 경우 갱신
                if stack_code not in latest_data_per_stack or item.get('mesure_dt') > latest_data_per_stack[stack_code].get('mesure_dt'):
                    latest_data_per_stack[stack_code] = item
        
        if not latest_data_per_stack:
            return {'error': '데이터는 있으나, 배출구 정보를 찾을 수 없습니다.'}

        # 결과 포맷팅
        results = {}
        # 배출구 코드를 기준으로 정렬하여 항상 같은 순서로 표시
        for stack_code in sorted(latest_data_per_stack.keys()):
            item = latest_data_per_stack[stack_code]
            nox_value = item.get('nox_mesure_value')
            measure_time = item.get('mesure_dt')
            
            if nox_value is not None:
                try:
                    formatted_value = f"{float(nox_value):.2f}"
                except (ValueError, TypeError):
                    formatted_value = nox_value
            else:
                formatted_value = 'N/A'
                
            results[stack_code] = {'value': formatted_value, 'time': measure_time}
        
        return results
    except Exception as e:
        return {'error': 'API 호출/처리 오류'}

# --- [신규] 기상청 초단기 실황 API 호출 함수 ---
@st.cache_data(ttl=600) # 10분마다 데이터 새로고침
def get_kma_weather_data():
    """
    기상청 초단기 실황 API를 호출하여 현재 날씨 정보를 가져옵니다.
    """
    # 1. API 요청 정보 설정
    # 사용자의 다른 API와 동일한 서비스 키를 사용합니다.
    service_key = "88fbd212b04f2b9e7f9e678d1fc903496f49f27c7af7d562dace83486c3effef"
    url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"
    url = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"
    
    # 2. 요청 시각 설정 (API는 매 시 30분에 업데이트되므로, 45분 이전에는 이전 시각 데이터 조회)
    kst = ZoneInfo('Asia/Seoul')
    now = datetime.now(kst)
    if now.minute < 45:
        target_time = now - timedelta(hours=1)
    else:
        target_time = now
        
    base_date = target_time.strftime('%Y%m%d')
    base_time = target_time.strftime('%H00')

    # 3. 요청 파라미터 설정 (인천광역시 부평구 좌표)
    params = {
        'serviceKey': service_key,
        'pageNo': '1',
        'numOfRows': '10', # 필요한 데이터는 8개이므로 10개로 충분
        'dataType': 'JSON',
        'base_date': base_date,
        'base_time': base_time,
        'nx': '55', # 인천광역시 격자 X좌표
        'ny': '127'  # 인천광역시 격자 Y좌표
    }
    
    try:
        # 4. API 호출
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # 5. 데이터 파싱
        items = data.get('response', {}).get('body', {}).get('items', {}).get('item', [])
        
        if not items:
            return {'error': 'API에서 날씨 데이터를 수신하지 못했습니다.'}

        weather_info = {cat: item.get('obsrValue') for item in items if (cat := item.get('category'))}

        return {
            'temp': float(weather_info.get('T1H', 0)),      # 기온
            'humidity': float(weather_info.get('REH', 0)),  # 습도
            'wind_speed': float(weather_info.get('WSD', 0)),# 풍속
            'wind_deg': float(weather_info.get('VEC', 0)),   # 풍향
            'pty': weather_info.get('PTY')                  # 강수형태
        }
    except requests.exceptions.RequestException:
        return {'error': '날씨 API 호출에 실패했습니다.'}
    except Exception as e:
        return {'error': f'날씨 데이터 처리 중 오류 발생: {e}'}

# --- [신규] 풍향 각도를 16방위 문자로 변환하는 함수 ---
def deg_to_compass(deg):
    """풍향 각도를 16방위 문자(예: 북서)로 변환합니다."""
    if deg is None:
        return ""
    # 각도를 0-360 범위로 정규화
    deg = deg % 360
    val = int((deg / 22.5) + 0.5)
    # 16방위 배열
    arr = ["북", "북북동", "북동", "동북동", "동", "동남동", "남동", "남남동", "남", "남남서", "남서", "서남서", "서", "서북서", "북서", "북북서"]
    return arr[(val % 16)]

# --- [신규] PTY 코드를 날씨 정보로 변환하는 함수 ---
def pty_to_weather(pty_code):
    """기상청 PTY 코드(강수형태)를 아이콘과 설명으로 변환합니다."""
    if pty_code is None or pty_code == '0':
        return "☀️", "맑음" # 강수 없음은 '맑음'으로 표시 (가정)
    
    pty_map = {
        '1': ("🌧️", "비"),
        '2': ("🌨️", "비/눈"),
        '3': ("❄️", "눈"),
        '5': ("💧", "빗방울"),
        '6': ("🌨️", "눈날림"),
        '7': ("❄️", "눈날림")
    }
    return pty_map.get(str(pty_code), ("-", "정보 없음"))

# --- [신규] 구글 드라이브 파일 다운로드 함수 ---
@st.cache_data(ttl=600)
def download_excel_from_gdrive(file_id):
    """구글 드라이브에서 엑셀 파일을 다운로드하여 메모리 버퍼(BytesIO)로 반환합니다."""
    try:
        creds_dict = st.secrets["gcp_service_account"]
        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        service = build('drive', 'v3', credentials=credentials)
        request = service.files().get_media(fileId=file_id)
        file_io = io.BytesIO()
        downloader = MediaIoBaseDownload(file_io, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        file_io.seek(0)
        return file_io
    except Exception as e:
        st.error(f"구글 드라이브 연동 중 오류가 발생했습니다: {e}")
        return None

# 4. 데이터 로드 함수
@st.cache_data(ttl=600) # 10분마다 데이터 새로고침
def load_data(view_type, year=None, month=None, date_obj=None):
    """선택된 기간(일별/월별/연별)을 기준으로 엑셀 파일에서 운영 데이터를 집계합니다."""
    # --- 1. 조회 기간 및 파일 경로 설정 ---
    kst = ZoneInfo('Asia/Seoul')
    today = datetime.now(kst).date()
    yesterday_date = today - timedelta(days=1)

    if view_type == '일별':
        if not date_obj: return create_empty_df(), create_empty_summary_data(), None
        start_date = end_date = date_obj
        query_year = start_date.year
    elif view_type == '월별':
        if not year or not month: return create_empty_df(), create_empty_summary_data(), None
        start_date = datetime(year, month, 1).date()
        # 해당 월의 마지막 날 계산
        next_month = datetime(year, month, 1) + timedelta(days=32)
        end_of_month = (next_month.replace(day=1) - timedelta(days=1)).date()
        # 조회 종료일이 어제를 넘지 않도록 조정
        end_date = min(end_of_month, yesterday_date)
        query_year = year
    elif view_type == '연별':
        if not year: return create_empty_df(), create_empty_summary_data(), None
        start_date = datetime(year, 1, 1).date()
        # 조회 종료일이 어제를 넘지 않도록 조정
        end_date = min(datetime(year, 12, 31).date(), yesterday_date) # .date() 추가
        query_year = year
    else:
        return create_empty_df(), create_empty_summary_data(), create_empty_external_heat_data(), None

    # --- 데이터 소스 설정 (구글 드라이브 File ID) ---
    file_ids = {
        2025: st.secrets["gdrive_files"]["file_id_2025"],
        2026: st.secrets["gdrive_files"]["file_id_2026"]
    }

    if query_year in file_ids:
        file_id = file_ids[query_year]
    else:
        st.error(f"{query_year}년도의 구글 드라이브 파일 ID가 설정되지 않았습니다.")
        return create_empty_df(), create_empty_summary_data(), create_empty_external_heat_data(), None

    # --- 2. 데이터 읽기 ---
    try:
        # 구글 드라이브에서 파일 다운로드
        file_data = download_excel_from_gdrive(file_id)
        if file_data is None:
            return create_empty_df(), create_empty_summary_data(), create_empty_external_heat_data(), None
            
        daily_sheet_df = pd.read_excel(file_data, sheet_name="일별", header=None, engine='openpyxl')
        file_data.seek(0) # 버퍼 위치 초기화 후 두 번째 시트 읽기
        source_sheet_df = pd.read_excel(file_data, sheet_name="운전실적_원본", header=None, engine='openpyxl')
        
    except Exception as e:
        st.error(f"데이터 로딩 중 오류가 발생했습니다: {e}")
        return create_empty_df(), create_empty_summary_data(), create_empty_external_heat_data(), None

    # --- 3. 행 범위 계산 및 데이터 슬라이싱 ---
    HEADER_OFFSET = 5
    start_row_idx = start_date.timetuple().tm_yday + HEADER_OFFSET - 1
    end_row_idx = end_date.timetuple().tm_yday + HEADER_OFFSET - 1
    
    daily_data_slice = daily_sheet_df.iloc[start_row_idx : end_row_idx + 1]
    source_data_slice = source_sheet_df.iloc[start_row_idx : end_row_idx + 1]

    # --- 4-A. 폐수 관련 총량 집계 (누적값 기반 사용량 계산) ---
    try:
        def _safe_to_numeric(value):
            """Helper to convert a single value to numeric, returning 0 for errors or NaN."""
            num = pd.to_numeric(value, errors='coerce')
            return 0 if pd.isna(num) else num

        # 1. 기간 시작 전날의 누적값 가져오기
        # 파일 내에서 항상 start_row_idx - 1 행에 조회 시작일의 전날(연초인 경우 작년 12월 31일) 누적값이 위치합니다.
        prev_row_idx = start_row_idx - 1
        val_I_prev = _safe_to_numeric(source_sheet_df.iloc[prev_row_idx, column_index_from_string('I') - 1])
        val_C_prev = _safe_to_numeric(source_sheet_df.iloc[prev_row_idx, column_index_from_string('C') - 1])
        val_D_prev = _safe_to_numeric(source_sheet_df.iloc[prev_row_idx, column_index_from_string('D') - 1])
        val_H_prev = _safe_to_numeric(source_sheet_df.iloc[prev_row_idx, column_index_from_string('H') - 1])
        val_G_prev = _safe_to_numeric(source_sheet_df.iloc[prev_row_idx, column_index_from_string('G') - 1])
        val_E_prev = _safe_to_numeric(source_sheet_df.iloc[prev_row_idx, column_index_from_string('E') - 1])
        val_F_prev = _safe_to_numeric(source_sheet_df.iloc[prev_row_idx, column_index_from_string('F') - 1])

        # 2. 사용량 계산 (계량기 교체 및 리셋을 고려하여 일별 사용량 합산)
        def _calculate_cumulative_usage(prev_val, col_letter):
            col_idx = column_index_from_string(col_letter) - 1
            s = pd.to_numeric(source_data_slice.iloc[:, col_idx], errors='coerce').dropna()
            
            total = 0
            current_prev = prev_val
            for val in s:
                # 누적값이 이전 값의 절반 이하로 떨어지면(큰 폭의 감소) 계량기 리셋/교체로 판단
                # 소폭 감소(단순 오기입 수정)는 단순 차이값(음수)으로 반영하여 오차를 상쇄시킴
                if val < current_prev * 0.5 and current_prev > 0:
                    total += val
                else:
                    total += (val - current_prev)
                current_prev = val
            return max(0, total)

        total_water_supply = _calculate_cumulative_usage(val_I_prev, 'I')
        total_pure_water = _calculate_cumulative_usage(val_C_prev, 'C') + _calculate_cumulative_usage(val_D_prev, 'D')
        total_discharge = _calculate_cumulative_usage(val_H_prev, 'H')
        total_wastewater_power = _calculate_cumulative_usage(val_G_prev, 'G')
        total_domestic_water = _calculate_cumulative_usage(val_E_prev, 'E')
        total_cooling_water = _calculate_cumulative_usage(val_F_prev, 'F')
        
        summary_data = {
            '총 상수도 사용량': total_water_supply,
            '순수 생산량': total_pure_water,
            '방류수량': total_discharge,
            '폐수전력 사용량': total_wastewater_power,
            '생활용수량': total_domestic_water,
            '1차 냉각수량': total_cooling_water
        }
    except Exception as e:
        st.warning(f"폐수 관련 총량 데이터를 집계하는 중 오류 발생: {e}")
        summary_data = create_empty_summary_data()

    # --- 4-C. 외부수열 관련 총량 집계 ---
    external_heat_summary = create_empty_external_heat_data() # Initialize with empty data

    external_heat_cols = {
        '남부소각장': 'AD',
        'ERG': 'AE',
        'SRF': 'AF',
        '인천종합에너지': 'AG',
        '안산도시개발': 'AH'
    }

    try:
        for facility_name, col_letter in external_heat_cols.items():
            col_idx = column_index_from_string(col_letter) - 1
            heat_value = pd.to_numeric(source_data_slice.iloc[:, col_idx], errors='coerce').sum()
            external_heat_summary[facility_name] = heat_value
    except Exception as e:
        st.warning(f"외부수열 관련 총량 데이터를 집계하는 중 오류 발생: {e}")
        external_heat_summary = create_empty_external_heat_data() # Reset to empty if error

    # --- 4-B. 설비별 데이터 집계 ---
    final_data = {}
    facility_cols = {'CHP': 'AH', 'PLB #1': 'AI', 'PLB #2': 'AJ', 'PLB #3': 'AK'}
    heat_prod_cols = {'CHP': 'W', 'PLB #1': 'X', 'PLB #2': 'Y', 'PLB #3': 'Z'}
    plb_lng_cols = {'PLB #1': 'N', 'PLB #2': 'O', 'PLB #3': 'P'}

    # [신규] 2025, 2026년 NOx 계산을 위한 사전 집계
    use_ac_col_for_nox = query_year in [2025, 2026]
    total_nox_from_ac = 0

    if use_ac_col_for_nox:
        try:
            # AC열에서 해당 기간의 총 NOx 배출량 합산
            nox_col_idx = column_index_from_string('AC') - 1
            total_nox_from_ac = pd.to_numeric(source_data_slice.iloc[:, nox_col_idx], errors='coerce').sum()
        except Exception as e:
            st.warning(f"{query_year}년 NOx 배출량 사전 집계 중 오류 발생: {e}")
            use_ac_col_for_nox = False # 오류 발생 시 기존 방식으로 계산하도록 플래그 변경

    for facility_name, daily_col_letter in facility_cols.items():
        try:
            facility_data_row = {}
            daily_col_idx = column_index_from_string(daily_col_letter) - 1

            # 가동 시간, 열 생산량 (기간 합산)
            facility_data_row['가동 시간 (hr)'] = pd.to_numeric(daily_data_slice[daily_col_idx], errors='coerce').sum()
            
            heat_prod_col_idx = column_index_from_string(heat_prod_cols[facility_name]) - 1
            facility_data_row['열 생산량 (Gcal)'] = pd.to_numeric(source_data_slice[heat_prod_col_idx], errors='coerce').sum()

            # 연누적 가동 시간 (기간의 마지막 날 기준 누적치)
            CUMULATIVE_START_ROW_IDX = 1 + HEADER_OFFSET - 1
            cumulative_end_row_idx = end_row_idx
            facility_data_row['연누적 가동 시간 (hr)'] = pd.to_numeric(daily_sheet_df.iloc[CUMULATIVE_START_ROW_IDX : cumulative_end_row_idx + 1, daily_col_idx], errors='coerce').sum()

            # 온실가스 및 NOx 배출량 (기간 합산)
            total_ghg = 0
            total_nox = 0

            if facility_name == 'CHP':
                lng_usage_col_idx = column_index_from_string('M') - 1
                lng_usages = pd.to_numeric(source_data_slice[lng_usage_col_idx], errors='coerce').fillna(0)
                facility_data_row['LNG 사용량 (m³)'] = lng_usages.sum()
                
                # 온실가스 배출량 (tCO₂) = LNG사용량 * ((56100*38.9*0.995/10^6*1)+(1*38.9*1/10^6*21)+(0.1*38.9*1/10^6*310))/1000
                term1 = lng_usages * 56100 * 38.9 * 0.995 / 1_000_000
                term2 = lng_usages * 1 * 38.9 * 1 * 21 / 1_000_000
                term3 = lng_usages * 0.1 * 38.9 * 1 * 310 / 1_000_000
                total_ghg = ((term1 + term2 + term3) / 1000).sum()

                # NOx 배출량 계산
                if use_ac_col_for_nox:
                    # 2025, 2026년 데이터는 AC열의 값을 사용
                    total_nox = total_nox_from_ac
                else:
                    # 기존 방식: NOx 배출량은 TMS로 실시간 측정 중임을 표시
                    total_nox = "TMS 측정 중"

            else: # PLB
                lng_col_letter = plb_lng_cols[facility_name]
                lng_usage_col_idx = column_index_from_string(lng_col_letter) - 1
                lng_usages = pd.to_numeric(source_data_slice[lng_usage_col_idx], errors='coerce').fillna(0)
                facility_data_row['LNG 사용량 (m³)'] = lng_usages.sum()
                
                # GHG
                # 온실가스 배출량 (tCO₂) = LNG사용량 * ((56100*38.9*1/10^6*1)+(1*38.9*1/10^6*21)+(0.1*38.9*1/10^6*310))/1000
                term1 = lng_usages * 56100 * 38.9 * 1 / 1_000_000
                term2 = lng_usages * 1 * 38.9 * 1 * 21 / 1_000_000
                term3 = lng_usages * 0.1 * 38.9 * 1 * 310 / 1_000_000
                total_ghg = ((term1 + term2 + term3) / 1000).sum()
                
                # NOx 배출량 계산
                # PLB는 항상 기존 방식으로 계산
                total_nox = ((lng_usages / 1000) * 3.7 * (1 - 0.122)).sum()

            facility_data_row['온실가스 배출량 (tCO₂)'] = total_ghg
            facility_data_row['NOx 배출량 (kg)'] = total_nox
            
            final_data[facility_name] = facility_data_row

        except Exception as e:
            st.warning(f"'{facility_name}' 데이터 집계 중 오류 발생: {e}")
            final_data[facility_name] = {
                '가동 시간 (hr)': 0, '연누적 가동 시간 (hr)': 0, '열 생산량 (Gcal)': 0,
                '온실가스 배출량 (tCO₂)': 0, 'NOx 배출량 (kg)': 0
            }

    # --- 5. 최종 데이터프레임 생성 ---
    df = pd.DataFrame.from_dict(final_data, orient='index')
    df.index.name = '구분' # Changed from '구분' to '구분'
    ordered_columns = ['가동 시간 (hr)', '연누적 가동 시간 (hr)', '열 생산량 (Gcal)', 'LNG 사용량 (m³)', '온실가스 배출량 (tCO₂)', 'NOx 배출량 (kg)']
    df = df[ordered_columns] # Ensure all columns exist before indexing
    return df, summary_data, external_heat_summary, end_date

def create_empty_df():
    """데이터 로드 실패 시 사용할 빈 데이터프레임을 생성합니다."""
    columns = {
        '가동 시간 (hr)': [0.0] * 4,
        '연누적 가동 시간 (hr)': [0.0] * 4,
        '열 생산량 (Gcal)': [0.0] * 4,
        'LNG 사용량 (m³)': [0.0] * 4,
        '온실가스 배출량 (tCO₂)': [0.0] * 4,
        'NOx 배출량 (kg)': [0.0] * 4
    }
    index = ['CHP', 'PLB #1', 'PLB #2', 'PLB #3']
    return pd.DataFrame(columns, index=pd.Index(index, name='구분'))

def create_empty_summary_data():
    """폐수 관련 데이터 로드 실패 시 사용할 빈 딕셔너리를 생성합니다."""
    return {
        '총 상수도 사용량': 0,
        '순수 생산량': 0,
        '방류수량': 0,
        '폐수전력 사용량': 0,
        '생활용수량': 0,
        '1차 냉각수량': 0
    }

# --- [신규] 외부수열 관련 데이터 로드 실패 시 사용할 빈 딕셔너리를 생성합니다. ---
def create_empty_external_heat_data():
    """외부수열 관련 데이터 로드 실패 시 사용할 빈 딕셔너리를 생성합니다."""
    return {
        '남부소각장': 0,
        'ERG': 0,
        'SRF': 0,
        '인천종합에너지': 0,
        '안산도시개발': 0
    }

# --- [신규] 금일 운영 요약 팝업(Dialog) 함수 ---
@st.dialog("📋 금일 운영 요약 리포트")
def show_today_summary_dialog(df, summary, external_heat_data, date_str):
    st.markdown(f"**기준일:** {date_str} (금일)")
    
    st.markdown("#### 📈 대기배출시설")
    try:
        # 가동 시간이 0보다 큰 설비만 필터링
        active_mask = pd.to_numeric(df['가동 시간 (hr)'], errors='coerce') > 0
        active_facs = df[active_mask]
    except Exception:
        active_facs = pd.DataFrame()
        
    if not active_facs.empty:
        display_df = active_facs[['가동 시간 (hr)', '열 생산량 (Gcal)', 'LNG 사용량 (m³)']]
        # 데이터 포맷(천 단위 콤마), 값 및 헤더 가운데 정렬 스타일 적용
        styled_df = display_df.style.format("{:,.1f}").set_properties(**{'text-align': 'center'}).set_table_styles([dict(selector='th', props=[('text-align', 'center')])])
        st.dataframe(
            styled_df, 
            use_container_width=True
        )
    else:
        st.info("금일 가동 기록이 있는 대기배출시설이 없습니다.")
        
    st.markdown("#### 💧 폐수배출시설")
    cols = st.columns(3)
    metrics = list(summary.items())
    for i, (k, v) in enumerate(metrics[:6]):
        unit = "kWh" if "전력" in k else "m³"
        with cols[i % 3]:
            val_str = f"{int(v):,}" if k in ["총 상수도 사용량", "순수 생산량", "방류수량"] else f"{float(v):,.1f}"
            st.metric(k, f"{val_str} {unit}")
            
    st.markdown("#### ♨️ 외부수열 현황")
    eh_cols = st.columns(3)
    eh_metrics = list(external_heat_data.items())
    for i, (k, v) in enumerate(eh_metrics):
        with eh_cols[i % 3]:
            st.metric(k, f"{v:,.1f} Gcal")

# [테스트용] 위드인천에너지 NOx 데이터 조회 (실시간 탭용)
# API 함수는 인자를 사용하지 않으므로 None을 전달하여 항상 최신 데이터를 가져옵니다.
test_nox_value = get_with_incheon_energy_test_nox_from_api(None)

# --- 실시간 모니터링 탭 ---

# [테스트용] 위드인천에너지 NOx 데이터 조회 (실시간 탭용)
# API 함수는 인자를 사용하지 않으므로 None을 전달하여 항상 최신 데이터를 가져옵니다.
test_nox_value = get_with_incheon_energy_test_nox_from_api(None)

if selected_tab == "실시간 모니터링":
    # --- 실시간 NOx 농도 현황 ---
    left_col, right_col = st.columns([0.3, 0.7])

    with left_col:
        # --- 실시간 기준 시각 표시 ---
        with st.container(border=True):
            time_col, weather_col = st.columns(2)

            with time_col:
                kst = ZoneInfo('Asia/Seoul')
                now = datetime.now(kst)

                # 요일을 한글로 변환하기 위한 맵
                weekday_map = {
                    0: "월요일", 1: "화요일", 2: "수요일", 3: "목요일", 4: "금요일", 5: "토요일", 6: "일요일"
                }
                korean_weekday = weekday_map[now.weekday()]

                # 날짜 및 시간 문자열 포맷팅
                display_date_str = now.strftime(f"%Y.%m.%d {korean_weekday}")
                display_time_str = now.strftime("%H:%M")

                st.markdown(f"""
                <div style="display: flex; flex-direction: column; justify-content: center; height: 100%; min-height: 110px; padding-top: 25px;">
                    <h4 style='font-weight: 600; margin-bottom: -10px; white-space: nowrap;'>{display_date_str}</h4>
                    <h3 style='text-align: center; margin-top: -10px; font-weight: 400;'>{display_time_str}</h3>
                </div>
                """, unsafe_allow_html=True)

            with weather_col:
                # --- 날씨 정보 표시 (기상청 API) ---
                weather_data = get_kma_weather_data()

                if 'error' in weather_data:
                    st.warning(f"날씨 정보: {weather_data['error']}")
                else:
                    weather_icon, weather_desc = pty_to_weather(weather_data.get('pty'))
                    wind_direction = deg_to_compass(weather_data.get('wind_deg'))
                    
                    st.markdown(f"{weather_icon} **날씨**: {weather_desc}")
                    st.markdown(f"🌡️ **온도**: {weather_data.get('temp', 'N/A'):.1f}°C")
                    st.markdown(f"💧 **습도**: {weather_data.get('humidity', 'N/A')}%")
                    st.markdown(f"💨 **풍속**: {weather_data.get('wind_speed', 'N/A'):.1f} m/s ({wind_direction})")

        st.markdown("<br>", unsafe_allow_html=True) # 박스와 다음 컨텐츠 사이 여백

        # --- 1. 위드인천에너지 NOx 농도 ---
        st.markdown("### 🏭 위드인천에너지")
        with st.container(border=True):
            # 위드인천에너지 NOx 값을 표시
            if isinstance(test_nox_value, dict):
                if '잘못된 사업장 코드' in str(test_nox_value.get('value', '')):
                    st.error(f"**데이터 식별 오류:** {test_nox_value['value']}")
                else:
                    st.metric(label="배출구 #1 NOx 농도 (ppm)", value=test_nox_value.get('value', 'N/A'))
                    time_str = test_nox_value.get('time')
                    if time_str and ' ' in time_str:
                        st.caption(f"측정시각: {time_str.split(' ')[-1]}")
            else:
                # "조회 불가" 등 문자열 값 처리
                st.metric(label="배출구 #1 NOx 농도 (ppm)", value=test_nox_value)

        # --- 2. 인천종합에너지 NOx 농도 ---
        st.markdown("### 🏭 인천종합에너지")
        incheon_total_nox_data = get_incheon_total_energy_nox_from_api()
        with st.container(border=True):
            if 'error' in incheon_total_nox_data:
                st.error(f"**데이터 조회 오류:** {incheon_total_nox_data['error']}")
            elif not incheon_total_nox_data:
                st.warning("인천종합에너지 데이터를 가져올 수 없습니다.")
            else:
                # 배출구 코드를 기준으로 정렬하여 항상 같은 순서로 표시
                sorted_stacks = sorted(incheon_total_nox_data.keys())

                if sorted_stacks:
                    cols = st.columns(len(sorted_stacks))
                    for i, stack_code in enumerate(sorted_stacks):
                        with cols[i]:
                            data = incheon_total_nox_data[stack_code]
                            st.metric(label=f"배출구 #{stack_code} NOx 농도 (ppm)", value=data.get('value', 'N/A'))
                            time_str = data.get('time')
                            if time_str and ' ' in time_str:
                                st.caption(f"측정시각: {time_str.split(' ')[-1]}")
                else:
                    st.info("조회된 배출구 데이터가 없습니다.")

    with right_col:
        # --- 종합 누적 현황 ---
        kst = ZoneInfo('Asia/Seoul')
        now_date = datetime.now(kst).date()
        
        # 6개 기간 데이터 로드
        target_date_prev2 = (now_date - timedelta(days=2))
        target_date_prev1 = (now_date - timedelta(days=1))
        
        df_prev_day, sum_prev_day, eh_prev_day, _ = load_data('일별', date_obj=target_date_prev2)
        
        df_today, sum_today, eh_today, _ = load_data('일별', date_obj=target_date_prev1)
        
        lm_year = now_date.year if now_date.month > 1 else now_date.year - 1
        lm_month = now_date.month - 1 if now_date.month > 1 else 12
        df_prev_month, sum_prev_month, eh_prev_month, _ = load_data('월별', year=lm_year, month=lm_month)
        
        df_this_month, sum_this_month, eh_this_month, _ = load_data('월별', year=now_date.year, month=now_date.month)
        
        df_prev_year, sum_prev_year, eh_prev_year, _ = load_data('연별', year=now_date.year - 1)
        
        df_this_year, sum_this_year, eh_this_year, _ = load_data('연별', year=now_date.year)

        periods = [
            ("전일", df_prev_day, sum_prev_day, target_date_prev2.strftime("%Y.%m.%d")),
            ("금일", df_today, sum_today, target_date_prev1.strftime("%Y.%m.%d")),
            ("전월", df_prev_month, sum_prev_month, f"{lm_year}년 {lm_month}월"),
            ("금월", df_this_month, sum_this_month, f"{now_date.year}년 {now_date.month}월"),
            ("전년", df_prev_year, sum_prev_year, f"{now_date.year - 1}년"),
            ("금년", df_this_year, sum_this_year, f"{now_date.year}년")
        ]

        # --- [신규] 금일 운영 요약 창 띄우기 버튼 ---
        if st.button("📋 대기/폐수 금일 운영 요약 창 띄우기", use_container_width=True, type="primary"):
            show_today_summary_dialog(df_today, sum_today, eh_today, target_date_prev1.strftime("%Y.%m.%d"))

        # --- [신규] 위드인천에너지 NOx 농도 알림 ---
        if isinstance(test_nox_value, dict) and 'value' in test_nox_value:
            try:
                current_nox = float(test_nox_value['value'])
                if current_nox > 20.0:
                    st.error(f"🚨 **[NOx 농도 경고]** 위드인천에너지의 질소산화물(NOx) 농도가 {current_nox} ppm으로 20 ppm을 초과했습니다!")
                else:
                    st.success(f"✅ **[NOx 농도 정상]** 위드인천에너지의 질소산화물(NOx) 농도({current_nox} ppm)가 정상 범위입니다.")
            except (ValueError, TypeError):
                pass

        # st.metric 컴팩트 스타일 및 네온 효과 적용
        st.markdown("""
            <style>
                div[data-testid="stMetric"] {
                    margin-bottom: -10px !important;
                }
                [data-testid="stMetric"] label {
                    color: #aaaaaa;
                }
                [data-testid="stMetric"] [data-testid="stMetricValue"] {
                    font-size: 1.5rem !important;
                    font-weight: 600 !important;
                }

                /* 네온사인 효과를 위한 키프레임 애니메이션 */
                @keyframes pulse-glow-blue {
                    0%   { box-shadow: 0 0 5px #00c0ff, 0 0 8px #00c0ff; }
                    50%  { box-shadow: 0 0 20px #00c0ff, 0 0 30px #00c0ff; }
                    100% { box-shadow: 0 0 5px #00c0ff, 0 0 8px #00c0ff; }
                }

                /* '금일'과 '금월' 카드에 네온 효과 적용 */
                /* id='card-marker' 뒤에 오는 stHorizontalBlock 내의 2번째와 4번째 카드 컨테이너를 선택 */
                #card-marker ~ div[data-testid="stHorizontalBlock"] > div:nth-child(2) > div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlockBorderWrapper"] > div,
                #card-marker ~ div[data-testid="stHorizontalBlock"] > div:nth-child(4) > div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlockBorderWrapper"] > div {
                    animation: pulse-glow-blue 2.5s infinite ease-in-out;
                }
            </style>
        """, unsafe_allow_html=True)
        
        # 1. 대기/폐수 선택
        facility_type = st.segmented_control("시설 구분", ["대기배출시설", "폐수배출시설"], default="대기배출시설", selection_mode="single", key="fac_type_select")
        if not facility_type: facility_type = "대기배출시설"

        if facility_type == "대기배출시설":
            with st.container(border=True):
                st.markdown("<div style='font-size: 0.95rem; font-weight: 600; color: #cccccc; margin-bottom: 5px;'>⚙️ 조회 옵션 설정</div>", unsafe_allow_html=True)
                opt_cols = st.columns([1, 1.5])
                with opt_cols[0]:
                    selected_facility = st.segmented_control(
                        "설비 선택",
                        ["CHP", "PLB #1", "PLB #2", "PLB #3"],
                        default="CHP",
                        selection_mode="single",
                        key="facility_select_air"
                    )
                    if not selected_facility: selected_facility = "CHP"
                with opt_cols[1]:
                    selected_metric = st.segmented_control(
                        "항목 선택", 
                        ["가동 시간 (hr)", "열 생산량 (Gcal)", "LNG 사용량 (m³)", "온실가스 배출량 (tCO₂)", "NOx 배출량 (kg)"],
                        default="가동 시간 (hr)",
                        selection_mode="single",
                        key="metric_select_air"
                    )
                    if not selected_metric: selected_metric = "가동 시간 (hr)"
            
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown("<div id='card-marker'></div>", unsafe_allow_html=True)
            cols = st.columns(len(periods))
            
            icons = {
                "CHP": "🔥",
                "PLB #1": "🔥",
                "PLB #2": "🔥",
                "PLB #3": "🔥"
            }
            
            target_facilities = [selected_facility]
            
            for i, (period_name, p_df, _, p_title) in enumerate(periods):
                with cols[i]:
                    with st.container(border=True):
                        st.markdown(f"<div style='text-align: center; font-weight: 700; font-size: 1.2rem; color: #1f77b4;'>{period_name}</div>", unsafe_allow_html=True)
                        st.markdown(f"<div style='text-align: center; color: #aaaaaa; font-size: 0.85rem; margin-bottom: 10px;'>{p_title}</div>", unsafe_allow_html=True)
                        st.divider()
                        for fac in target_facilities:
                            val = 0
                            if not p_df.empty and fac in p_df.index:
                                val = p_df.loc[fac].get(selected_metric, 0)

                            if selected_metric == 'NOx 배출량 (kg)' and fac == 'CHP':
                                display_val = val if isinstance(val, str) else f"{int(val):,}"
                            else:
                                try:
                                    fmt = ",.0f" if selected_metric == 'NOx 배출량 (kg)' else ",.1f"
                                    display_val = f"{float(val):{fmt}}"
                                except (ValueError, TypeError):
                                    display_val = "0" if selected_metric == 'NOx 배출량 (kg)' else "0.0"
                                    val = 0

                            # 변동률 계산 및 HTML 생성
                            delta_html = ""
                            if i in [1, 3, 5]:  # 금일, 금월, 금년
                                _, prev_p_df, _, _ = periods[i - 1]
                                prev_val = 0
                                if not prev_p_df.empty and fac in prev_p_df.index:
                                    prev_val = prev_p_df.loc[fac].get(selected_metric, 0)

                                try:
                                    numeric_val = float(val)
                                    numeric_prev_val = float(prev_val)
                                    delta = numeric_val - numeric_prev_val
                                    delta_fmt = ",.0f" if selected_metric == 'NOx 배출량 (kg)' else ",.1f"
                                    
                                    if delta > 1e-9:
                                        delta_html = f" <span style='color: red; font-size: 0.8rem;'>▲{delta:{delta_fmt}}</span>"
                                    elif delta < -1e-9:
                                        delta_html = f" <span style='color: #17A589; font-size: 0.8rem;'>▼{abs(delta):{delta_fmt}}</span>"
                                except (ValueError, TypeError):
                                    pass

                            label_text = f"{icons.get(fac, '')} {fac}"
                            st.markdown(f"""<div style="margin-bottom: -10px;"><div style="color: #aaaaaa;">{label_text}</div><div style="font-size: 1.5rem; font-weight: 600;">{display_val}{delta_html}</div></div>""", unsafe_allow_html=True)
                            st.markdown("<div style='height: 28px'></div>", unsafe_allow_html=True)

        else: # 폐수배출시설
            wastewater_items_cumulative = {
                "총 상수도 사용량": "m³",
                "순수 생산량": "m³",
                "방류수량": "m³",
                "폐수전력 사용량": "kWh",
                "생활용수량": "m³",
                "1차 냉각수량": "m³"
            }
            item_options = [f"{k} ({v})" for k, v in wastewater_items_cumulative.items()]
            
            with st.container(border=True):
                st.markdown("<div style='font-size: 0.95rem; font-weight: 600; color: #cccccc; margin-bottom: 5px;'>⚙️ 조회 옵션 설정</div>", unsafe_allow_html=True)
                selected_item = st.segmented_control(
                    "항목 선택", 
                    item_options,
                    default=item_options[0],
                    selection_mode="single",
                    label_visibility="collapsed",
                    key="metric_select_water"
                )
                if not selected_item: selected_item = item_options[0]
            
            # 항목 이름만 추출
            metric_key = selected_item.split(" (")[0]
            unit = selected_item.split(" (")[1].replace(")", "")
            
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown("<div id='card-marker'></div>", unsafe_allow_html=True)
            cols = st.columns(len(periods))
            
            for i, (period_name, _, p_sum, p_title) in enumerate(periods):
                with cols[i]:
                    with st.container(border=True):
                        st.markdown(f"<div style='text-align: center; font-weight: 700; font-size: 1.2rem; color: #1f77b4;'>{period_name}</div>", unsafe_allow_html=True)
                        st.markdown(f"<div style='text-align: center; color: #aaaaaa; font-size: 0.85rem; margin-bottom: 10px;'>{p_title}</div>", unsafe_allow_html=True)
                        st.divider()
                        val = p_sum.get(metric_key, 0)
                        try:
                                integer_metrics = ["총 상수도 사용량", "순수 생산량", "방류수량"]
                                if metric_key in integer_metrics:
                                    display_val = f"{int(val):,}"
                                    val = int(val) # Ensure val is numeric for delta calculation if it was not
                                else:
                                    display_val = f"{float(val):,.1f}"
                                    val = float(val)
                        except (ValueError, TypeError):
                                # Default to 0 or 0.0 based on the metric type
                            display_val = "0.0"
                            val = 0

                        # 변동률 계산 및 HTML 생성
                        delta_html = ""
                        if i in [1, 3, 5]:  # 금일, 금월, 금년
                            _, _, prev_p_sum, _ = periods[i - 1]
                            prev_val = prev_p_sum.get(metric_key, 0)

                            try:
                                delta = float(val) - float(prev_val)
                                integer_metrics = ["총 상수도 사용량", "순수 생산량", "방류수량"]
                                delta_fmt = ",.0f" if metric_key in integer_metrics else ",.1f"

                                if delta > 1e-9:
                                    delta_html = f" <span style='color: red; font-size: 0.8rem;'>▲{delta:{delta_fmt}}</span>"
                                elif delta < -1e-9:
                                    delta_html = f" <span style='color: #17A589; font-size: 0.8rem;'>▼{abs(delta):{delta_fmt}}</span>"
                            except (ValueError, TypeError):
                                pass
                        
                        label_text = f"사용량 ({unit})"
                        st.markdown(f"""<div style="margin-bottom: -10px;"><div style="color: #aaaaaa;">{label_text}</div><div style="font-size: 1.5rem; font-weight: 600;">{display_val}{delta_html}</div></div>""", unsafe_allow_html=True)
                        st.markdown("<div style='height: 28px'></div>", unsafe_allow_html=True)

if selected_tab == "기간별 운영 현황":
    # 상단에서 설정된 전역 조회 기준(global_params)으로 데이터 로드
    df, summary_data, external_heat_data, end_date = load_data(**global_params)

    if selected_sub_tab == "📈 대기배출시설 현황":
        # 5. 스타일 및 테이블 표시
        st.markdown(f"<h3 style='margin-top: -15px; margin-bottom: 15px;'>대기배출시설 및 방지시설 운영 현황 ({period_string})</h3>", unsafe_allow_html=True)
        
        # --- 가동 중인 설비에 네온 효과를 주기 위한 동적 CSS 생성 ---
        glow_style_css = ""
        if not df.empty:
            for i, (facility_name, data_row) in enumerate(df.iterrows()):
                # 가동 시간 (hr)이 0보다 크면 '가동 중'으로 판단
                is_running = pd.to_numeric(data_row.get('가동 시간 (hr)', 0), errors='coerce') > 0
                if is_running:
                    # i+1 번째 컬럼의 카드에 glow 애니메이션을 적용하는 CSS 규칙 추가
                    glow_style_css += f"""
                        div[data-testid="stHorizontalBlock"] > div:nth-child({i + 1}) > div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlockBorderWrapper"] > div {{
                            animation: pulse-glow 2s infinite ease-in-out;
                        }}
                    """

        # --- 카드 스타일 조정 ---
        # f-string의 중괄호({})가 CSS의 중괄호와 충돌하여 발생하는 오류를 피하기 위해 문자열을 분리하여 처리합니다.
        static_css = """
        <style>
            /* st.metric 컨테이너 자체의 상하 마진을 줄여 카드 내부를 더 콤팩트하게 */
            div[data-testid="stMetric"] {
                margin-bottom: -15px !important;
            }
            /* st.metric 내부의 레이블(label) 스타일 조정 */
            [data-testid="stMetric"] label {
                color: #aaaaaa; /* 레이블 색상을 더 연하게 */
            }
            /* st.metric 내부의 값(value) 스타일 조정 */
            [data-testid="stMetric"] [data-testid="stMetricValue"] {
                font-size: 1.7rem !important; /* 값의 글자 크기 조정 */
                font-weight: 600 !important;
            }

            /* 네온사인 효과를 위한 키프레임 애니메이션 */
            @keyframes pulse-glow {
                0% {{ box-shadow: 0 0 4px rgba(0, 212, 255, 0.7); }}
                50% {{ box-shadow: 0 0 16px rgba(0, 212, 255, 1); }}
                100% {{ box-shadow: 0 0 4px rgba(0, 212, 255, 0.7); }}
            }}
        """
        # 동적으로 생성된 CSS와 정적 CSS를 합칩니다.
        final_css = static_css + glow_style_css + "</style>"

        st.markdown(final_css, unsafe_allow_html=True)

        # 4개의 컬럼을 생성합니다.
        cols = st.columns(4)

        # 아이콘 딕셔너리
        icons = {
            "CHP": "🔥",
            "PLB #1": "🔥",
            "PLB #2": "🔥",
            "PLB #3": "🔥"
        }

        # 데이터프레임의 각 행(설비)을 순회하며 카드를 생성합니다.
        # df가 비어있지 않은지 확인
        if not df.empty:
            for i, (facility_name, data_row) in enumerate(df.iterrows()):
                with cols[i]:
                    with st.container(border=True):
                        # 설비명과 아이콘 (스타일 조정)
                        st.markdown(f"<div style='text-align: center; font-weight: 700; font-size: 1.2rem; margin-bottom: 15px;'>{icons.get(facility_name, '🏭')} {facility_name}</div>", unsafe_allow_html=True)
                        
                        # 메트릭 표시
                        st.metric(label="가동 시간 (hr)", value=f"{data_row.get('가동 시간 (hr)', 0):,.1f}")
                        st.metric(label="열 생산량 (Gcal)", value=f"{data_row.get('열 생산량 (Gcal)', 0):,.1f}")
                        st.metric(label="LNG 사용량 (m³)", value=f"{data_row.get('LNG 사용량 (m³)', 0):,.1f}")
                        st.metric(label="온실가스 배출량 (tCO₂)", value=f"{data_row.get('온실가스 배출량 (tCO₂)', 0):,.1f}")

                        # NOx 값은 문자열일 수 있으므로 별도 처리
                        nox_value = data_row.get('NOx 배출량 (kg)', 0)

                        if facility_name == 'CHP':
                            # CHP의 경우, 'TMS 측정 중' 텍스트 또는 정수형 배출량을 표시
                            display_nox = nox_value if isinstance(nox_value, str) else f"{int(nox_value):,}"
                            st.metric(label="NOx 배출량 (kg)", value=display_nox)
                        else:
                            # PLB의 경우 계산된 배출량(kg)을 표시
                            st.metric(label="NOx 배출량 (kg)", value=f"{int(nox_value):,}")
        else:
            st.warning("데이터를 불러올 수 없거나 데이터가 비어있습니다.")

    if selected_sub_tab == "💧 폐수배출시설 현황":
        # 6. 폐수배출시설 현황 # Removed date_select_col and title_col
        st.markdown(f"<h3 style='margin-top: -15px; margin-bottom: 15px;'>폐수배출시설 및 방지시설 운영 현황 ({period_string})</h3>", unsafe_allow_html=True)

        summary_cols = st.columns(6)

        # 폐수 관련 아이템 정의
        wastewater_items = {
            "총 상수도 사용량": {"icon": "🚰", "unit": "m³"},
            "순수 생산량": {"icon": "💧", "unit": "m³"},
            "방류수량": {"icon": "🌊", "unit": "m³"},
            "폐수전력 사용량": {"icon": "⚡️", "unit": "kWh"},
            "생활용수량": {"icon": "🚿", "unit": "m³"},
            "1차 냉각수량": {"icon": "❄️", "unit": "m³"}
        }

        # 딕셔너리를 리스트로 변환하여 순회
        wastewater_list = list(wastewater_items.items())

        for i, (item_name, details) in enumerate(wastewater_list): # Changed from `wastewater_list` to `wastewater_list_cumulative`
            with summary_cols[i]:
                with st.container(border=True):
                    # 아이템명과 아이콘
                    st.markdown(f"<div style='text-align: center; font-weight: 700; font-size: 1.2rem; margin-bottom: 15px;'>{details['icon']} {item_name}</div>", unsafe_allow_html=True)
                    # 메트릭 표시
                    val = summary_data.get(item_name, 0)
                    integer_metrics = ["총 상수도 사용량", "순수 생산량", "방류수량"]
                    if item_name in integer_metrics:
                        display_val = f"{int(val):,}"
                    else:
                        display_val = f"{float(val):,.1f}"
                    
                    st.metric(label=f"사용량 ({details['unit']})", value=display_val)

    elif selected_sub_tab == "♨️ 외부수열 현황":
        st.markdown(f"<h3 style='margin-top: -15px; margin-bottom: 15px;'>외부수열 운영 현황 ({period_string})</h3>", unsafe_allow_html=True)

        # --- 카드 스타일 조정 (대기배출시설과 유사하게) ---
        static_css = """
        <style>
            div[data-testid="stMetric"] {
                margin-bottom: -15px !important;
            }
            [data-testid="stMetric"] label {
                color: #aaaaaa;
            }
            [data-testid="stMetric"] [data-testid="stMetricValue"] {
                font-size: 1.7rem !important;
                font-weight: 600 !important;
            }
        </style>
        """
        st.markdown(static_css, unsafe_allow_html=True)

        cols = st.columns(len(external_heat_data)) # Create columns based on number of facilities
        
        if external_heat_data:
            for i, (facility_name, heat_value) in enumerate(external_heat_data.items()):
                with cols[i]:
                    with st.container(border=True):
                        st.markdown(f"<div style='text-align: center; font-weight: 700; font-size: 1.2rem; margin-bottom: 15px;'>♨️ {facility_name}</div>", unsafe_allow_html=True)
                        st.metric(label="수열량 (Gcal)", value=f"{heat_value:,.1f}")
        else:
            st.warning("외부수열 데이터를 불러올 수 없거나 데이터가 비어있습니다.")

    # --- 7. 월별/연별 운영 실적 추이 그래프 ---
    st.markdown("---")
    st.markdown("### 📊 월별/연별 운영 실적 추이 그래프")

    # 분석 대상은 상단 탭 선택에 따라 자동으로 결정됩니다.
    if "대기배출시설" in selected_sub_tab:
        analysis_target = "대기배출시설"
    elif "폐수배출시설" in selected_sub_tab:
        analysis_target = "폐수배출시설"
    elif "외부수열" in selected_sub_tab:
        analysis_target = "외부수열"
    else:
        analysis_target = "대기배출시설" # 기본값

    kst_now = datetime.now(ZoneInfo('Asia/Seoul'))
    current_year = kst_now.year
    years = list(range(2025, current_year + 1))
    
    chart_color = ["#1f77b4", "#ff4b4b"]
    chart_color = ["#17A589", "#ff4b4b"]

    # 외부수열을 포함한 모든 탭에서 분석 모드 라디오 버튼 표시
    if True:
        comp_type = st.radio("분석 모드", ["기간별 추이", "비교 분석"], horizontal=True, key="comp_type_radio")

        if comp_type == "기간별 추이":
            trend_df = pd.DataFrame()
            data_list = []
            labels = []

            # --- 1. Controls ---
            control_col, _ = st.columns(2)
            with control_col:
                sub_type = st.radio("그래프 조회 단위", ["월별", "연도별"], horizontal=True, key="sub_type_radio")

                if sub_type == "연도별":
                    yc1, yc2 = st.columns(2)
                    with yc1:
                        trend_start_year = st.selectbox("시작 연도", years, key="ts_y_yr")
                    with yc2:
                        trend_end_year = st.selectbox("종료 연도", years, index=len(years)-1, key="te_y_yr")
                else: # 월별
                    mc1, mc2 = st.columns(2)
                    with mc1:
                        st.markdown("**시작 월**")
                        sc1, sc2 = st.columns(2)
                        with sc1:
                            trend_start_year = st.selectbox("연도", years, key="ts_y_mo")
                        with sc2:
                            trend_start_month = st.selectbox("월", range(1, 13), key="ts_m_mo")
                    with mc2:
                        st.markdown("**종료 월**")
                        ec1, ec2 = st.columns(2)
                        with ec1:
                            trend_end_year = st.selectbox("연도", years, index=len(years)-1, key="te_y_mo")
                        with ec2:
                            trend_end_month = st.selectbox("월", range(1, 13), index=kst_now.month-1, key="te_m_mo")

            # --- 2. Data Loading ---
            if sub_type == "연도별":
                if trend_start_year <= trend_end_year:
                    for y in range(trend_start_year, trend_end_year + 1):
                        df_y, summary_y, external_heat_summary_y, _ = load_data('연별', year=y) # load_data 반환값 변경
                        if analysis_target == "대기배출시설":
                            y_df = df_y
                            if not y_df.empty:
                                heat = pd.to_numeric(y_df['열 생산량 (Gcal)'], errors='coerce').sum()
                                lng = pd.to_numeric(y_df['LNG 사용량 (m³)'], errors='coerce').sum()
                                ghg = pd.to_numeric(y_df['온실가스 배출량 (tCO₂)'], errors='coerce').sum()
                                labels.append(f"{y}년")
                                data_list.append({'열 생산량 (Gcal)': heat, 'LNG 사용량 (m³)': lng, '온실가스 배출량 (tCO₂)': ghg})
                        elif analysis_target == "폐수배출시설": # 폐수배출시설
                            summary = summary_y
                            if summary and any(summary.values()): # summary_y 사용
                                labels.append(f"{y}년")
                                data_list.append(summary)
                        elif analysis_target == "외부수열": # 외부수열
                            eh_summary = external_heat_summary_y
                            if eh_summary and any(eh_summary.values()):
                                labels.append(f"{y}년")
                                data_list.append(eh_summary)
                else:
                    st.warning("시작 연도가 종료 연도보다 늦습니다.")

            else: # 월별
                if trend_start_year < trend_end_year or (trend_start_year == trend_end_year and trend_start_month <= trend_end_month):
                    current_y, current_m = trend_start_year, trend_start_month
                    while (current_y < trend_end_year) or (current_y == trend_end_year and current_m <= trend_end_month):
                        df_m, summary_m, external_heat_summary_m, _ = load_data('월별', year=current_y, month=current_m) # load_data 반환값 변경
                        if analysis_target == "대기배출시설":
                            m_df = df_m
                            if not m_df.empty: # m_df 사용
                                heat = pd.to_numeric(m_df['열 생산량 (Gcal)'], errors='coerce').sum()
                                lng = pd.to_numeric(m_df['LNG 사용량 (m³)'], errors='coerce').sum()
                                ghg = pd.to_numeric(m_df['온실가스 배출량 (tCO₂)'], errors='coerce').sum()
                                labels.append(f"{current_y}년 {current_m}월")
                                data_list.append({'열 생산량 (Gcal)': heat, 'LNG 사용량 (m³)': lng, '온실가스 배출량 (tCO₂)': ghg})
                        elif analysis_target == "폐수배출시설": # 폐수배출시설
                            summary = summary_m
                            if summary and any(summary.values()): # summary_m 사용
                                labels.append(f"{current_y}년 {current_m}월")
                                data_list.append(summary)
                        elif analysis_target == "외부수열": # 외부수열
                            eh_summary = external_heat_summary_m
                            if eh_summary and any(eh_summary.values()):
                                labels.append(f"{current_y}년 {current_m}월")
                                data_list.append(eh_summary)
                        current_m += 1
                        if current_m > 12:
                            current_m = 1
                            current_y += 1
                else:
                    st.warning("시작 월이 종료 월보다 늦습니다.")

            if data_list:
                trend_df = pd.DataFrame(data_list, index=labels)
                trend_df.index = pd.Categorical(trend_df.index, categories=labels, ordered=True)
                trend_df.index.name = "기간"

            if not trend_df.empty:
                chart_col, table_col = st.columns([1.2, 0.8])

                with chart_col:
                    if analysis_target == "대기배출시설": # 대기배출시설 추이 그래프
                        trend_metric = st.segmented_control(
                            "그래프 지표 선택",
                            ['열 생산량 (Gcal)', 'LNG 사용량 (m³)', '온실가스 배출량 (tCO₂)'],
                            default='열 생산량 (Gcal)',
                            selection_mode="single",
                            key="trend_metric_select"
                        )
                        if not trend_metric: trend_metric = '열 생산량 (Gcal)'

                        st.markdown("<br>", unsafe_allow_html=True)
                        categories = list(trend_df.index)
                        data_values = [float(v) if pd.notna(v) else 0.0 for v in trend_df[trend_metric]]

                        # ECharts data with custom colors
                        echarts_data_items = []
                        for category, value in zip(categories, data_values):
                            if '2026' in category: # 2026년 데이터에 다른 색상 적용
                                color_js = """new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                                    { offset: 0, color: '#FF416C' },
                                    { offset: 1, color: '#FF4B2B' }
                                ])"""
                            else:
                                color_js = """new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                                    { offset: 0, color: '#36D1DC' },
                                    { offset: 1, color: '#5B86E5' }
                                ])"""

                            item_str = f"""{{
                                value: {value},
                                itemStyle: {{
                                    color: {color_js},
                                    borderRadius: [6, 6, 0, 0],
                                    shadowColor: 'rgba(0, 0, 0, 0.1)',
                                    shadowBlur: 10,
                                    shadowOffsetX: 0,
                                    shadowOffsetY: 5
                                }}
                            }}"""
                            echarts_data_items.append(item_str)

                        echarts_data_str = f"[{', '.join(echarts_data_items)}]"

                        html_code = f"""
                            <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
                            <div id="chart-container-trend" style="height: 400px; width: 100%;"></div>
                            <script>
                            var chartDom = document.getElementById('chart-container-trend');
                            var myChart = echarts.init(chartDom);
                            var option = {{
                                tooltip: {{ // 툴팁 디자인 개선
                                    trigger: 'axis',
                                    axisPointer: {{ type: 'shadow', shadowStyle: {{ color: 'rgba(0, 0, 0, 0.05)' }} }},
                                    backgroundColor: 'rgba(255, 255, 255, 0.95)',
                                    borderColor: '#edf2f7',
                                    borderWidth: 1,
                                    padding: [10, 15],
                                    textStyle: {{ color: '#2d3748', fontSize: 13 }},
                                    valueFormatter: (value) => parseFloat(value).toLocaleString(undefined, {{minimumFractionDigits: 1, maximumFractionDigits: 1}})
                                }},
                                legend: {{ data: ['{trend_metric}'], bottom: 0, icon: 'circle', textStyle: {{ color: '#4a5568', fontWeight: '600', fontSize: 13 }} }},
                                grid: {{ left: '3%', right: '4%', bottom: '12%', top: '15%', containLabel: true }},
                                xAxis: [{{ // X축 디자인 개선
                                    type: 'category',
                                    data: {categories},
                                    axisLine: {{ show: false }},
                                    axisTick: {{ show: false }},
                                    axisLabel: {{ color: '#718096', margin: 12, fontWeight: '500' }}
                                }}],
                                yAxis: [{{ // Y축 디자인 개선
                                    type: 'value',
                                    name: '{trend_metric}',
                                    nameTextStyle: {{ color: '#718096', padding: [0, 0, 0, 10], fontWeight: '500' }},
                                    splitLine: {{ lineStyle: {{ type: 'dashed', color: '#e2e8f0' }} }},
                                    axisLabel: {{ color: '#718096' }}
                                }}],
                                series: [
                                    {{
                                        name: '{trend_metric}',
                                        type: 'bar',
                                        barMaxWidth: 50, // 막대 최대 너비
                                        data: {echarts_data_str}
                                    }}
                                ]
                            }};
                            myChart.setOption(option);
                            window.addEventListener('resize', function() {{ myChart.resize(); }});
                            </script>
                        """
                        components.html(html_code, height=450)
                    elif analysis_target == "폐수배출시설": # 폐수배출시설 추이 그래프
                        wastewater_items_cumulative = {
                            "총 상수도 사용량": "m³", "순수 생산량": "m³", "방류수량": "m³",
                            "폐수전력 사용량": "kWh", "생활용수량": "m³", "1차 냉각수량": "m³"
                        }
                        wastewater_metrics = list(wastewater_items_cumulative.keys())

                        trend_metric_water = st.segmented_control(
                            "그래프 지표 선택",
                            wastewater_metrics,
                            default=wastewater_metrics[0],
                            selection_mode="single",
                            key="trend_metric_select_water"
                        )
                        if not trend_metric_water: trend_metric_water = wastewater_metrics[0]

                        st.markdown("<br>", unsafe_allow_html=True)
                        categories = list(trend_df.index)
                        data_values = [float(v) if pd.notna(v) else 0.0 for v in trend_df[trend_metric_water]]

                        # ECharts data with custom colors
                        echarts_data_items = []
                        for category, value in zip(categories, data_values):
                            if '2026' in category: # 2026년 데이터에 다른 색상 적용
                                color_js = """new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                                    { offset: 0, color: '#DA22FF' },
                                    { offset: 1, color: '#9733EE' }
                                ])"""
                            else:
                                color_js = """new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                                    { offset: 0, color: '#00b09b' },
                                    { offset: 1, color: '#96c93d' }
                                ])"""

                            item_str = f"""{{
                                value: {value},
                                itemStyle: {{
                                    color: {color_js},
                                    borderRadius: [6, 6, 0, 0],
                                    shadowColor: 'rgba(0, 0, 0, 0.1)',
                                    shadowBlur: 10,
                                    shadowOffsetX: 0,
                                    shadowOffsetY: 5
                                }}
                            }}"""
                            echarts_data_items.append(item_str)

                        echarts_data_str = f"[{', '.join(echarts_data_items)}]"

                        html_code = f"""
                            <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
                            <div id="chart-container-trend-water" style="height: 400px; width: 100%;"></div>
                            <script>
                            var chartDom = document.getElementById('chart-container-trend-water');
                            var myChart = echarts.init(chartDom);
                            var option = {{
                                tooltip: {{ // 툴팁 디자인 개선
                                    trigger: 'axis',
                                    axisPointer: {{ type: 'shadow', shadowStyle: {{ color: 'rgba(0, 0, 0, 0.05)' }} }},
                                    backgroundColor: 'rgba(255, 255, 255, 0.95)',
                                    borderColor: '#edf2f7',
                                    borderWidth: 1,
                                    padding: [10, 15],
                                    textStyle: {{ color: '#2d3748', fontSize: 13 }},
                                    valueFormatter: (value) => parseFloat(value).toLocaleString(undefined, {{minimumFractionDigits: 1, maximumFractionDigits: 1}})
                                }},
                                legend: {{ data: ['{trend_metric_water}'], bottom: 0, icon: 'circle', textStyle: {{ color: '#4a5568', fontWeight: '600', fontSize: 13 }} }},
                                grid: {{ left: '3%', right: '4%', bottom: '12%', top: '15%', containLabel: true }},
                                xAxis: [{{ // X축 디자인 개선
                                    type: 'category',
                                    data: {categories},
                                    axisLine: {{ show: false }},
                                    axisTick: {{ show: false }},
                                    axisLabel: {{ color: '#718096', margin: 12, fontWeight: '500' }}
                                }}],
                                yAxis: [{{ // Y축 디자인 개선
                                    type: 'value',
                                    name: '{trend_metric_water}',
                                    nameTextStyle: {{ color: '#718096', padding: [0, 0, 0, 10], fontWeight: '500' }},
                                    splitLine: {{ lineStyle: {{ type: 'dashed', color: '#e2e8f0' }} }},
                                    axisLabel: {{ color: '#718096' }}
                                }}],
                                series: [
                                    {{
                                        name: '{trend_metric_water}',
                                        type: 'bar',
                                        barMaxWidth: 50, // 막대 최대 너비
                                        data: {echarts_data_str}
                                    }}
                                ]
                            }};
                            myChart.setOption(option);
                            window.addEventListener('resize', function() {{ myChart.resize(); }});
                            </script>
                        """
                        components.html(html_code, height=450)
                    
                    elif analysis_target == "외부수열": # 외부수열 누적 막대그래프
                        all_facilities = ['남부소각장', 'ERG', 'SRF', '인천종합에너지', '안산도시개발']
                        fac_colors = {
                            '남부소각장': '#FF6B6B', 
                            'ERG': '#4ECDC4', 
                            'SRF': '#45B7D1', 
                            '인천종합에너지': '#F7D794', 
                            '안산도시개발': '#96CEB4'
                        }
                        
                        selected_facs = st.segmented_control(
                            "수열처 선택 (선택하지 않으면 전체 누적 표시)",
                            all_facilities,
                            default=[],
                            selection_mode="multi",
                            key="trend_fac_select_heat"
                        )
                        
                        facilities_to_plot = selected_facs if selected_facs else all_facilities

                        st.markdown("<br>", unsafe_allow_html=True)
                        categories = list(trend_df.index)
                        
                        series_data = []
                        for fac in facilities_to_plot:
                            if fac in trend_df.columns:
                                data_values = [float(v) if pd.notna(v) else 0.0 for v in trend_df[fac]]
                                series_data.append(f"""{{
                                    name: '{fac}',
                                    type: 'bar',
                                    stack: 'total',
                                    barMaxWidth: 50,
                                    itemStyle: {{ color: '{fac_colors[fac]}' }},
                                    data: {data_values}
                                }}""")
                        series_str = ",\n".join(series_data)
                        
                        html_code = f"""
                            <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
                            <div id="chart-container-trend-heat" style="height: 400px; width: 100%;"></div>
                            <script>
                            var chartDom = document.getElementById('chart-container-trend-heat');
                            var myChart = echarts.init(chartDom);
                            var option = {{
                                tooltip: {{
                                    trigger: 'axis',
                                    axisPointer: {{ type: 'shadow' }},
                                    backgroundColor: 'rgba(255, 255, 255, 0.95)',
                                    borderColor: '#edf2f7',
                                    borderWidth: 1,
                                    padding: [10, 15],
                                    textStyle: {{ color: '#2d3748', fontSize: 13 }},
                                    valueFormatter: (value) => parseFloat(value).toLocaleString(undefined, {{minimumFractionDigits: 1, maximumFractionDigits: 1}}) + ' Gcal'
                                }},
                                legend: {{ data: {facilities_to_plot}, bottom: 0, icon: 'circle', textStyle: {{ color: '#4a5568', fontWeight: '600', fontSize: 13 }} }},
                                grid: {{ left: '3%', right: '4%', bottom: '12%', top: '15%', containLabel: true }},
                                xAxis: [{{
                                    type: 'category',
                                    data: {categories},
                                    axisLine: {{ show: false }},
                                    axisTick: {{ show: false }},
                                    axisLabel: {{ color: '#718096', margin: 12, fontWeight: '500' }}
                                }}],
                                yAxis: [{{
                                    type: 'value',
                                    name: '수열량 (Gcal)',
                                    nameTextStyle: {{ color: '#718096', padding: [0, 0, 0, 10], fontWeight: '500' }},
                                    splitLine: {{ lineStyle: {{ type: 'dashed', color: '#e2e8f0' }} }},
                                    axisLabel: {{ color: '#718096' }}
                                }}],
                                series: [{series_str}]
                            }};
                            myChart.setOption(option);
                            window.addEventListener('resize', function() {{ myChart.resize(); }});
                            </script>
                        """
                        components.html(html_code, height=450)

                with table_col:
                    st.markdown("#### 📋 운영 실적 데이터")
                    st.dataframe(trend_df.style.format("{:,.1f}", na_rep="-"), use_container_width=True)

            else:
                st.info("선택한 기간에 해당하는 데이터가 없습니다.")

        elif comp_type == "비교 분석":
            if analysis_target == "대기배출시설":
                trend_metric = st.segmented_control(
                    "그래프 지표 선택",
                    ['열 생산량 (Gcal)', 'LNG 사용량 (m³)', '온실가스 배출량 (tCO₂)'],
                    default='열 생산량 (Gcal)',
                    selection_mode="single",
                    key="comp_metric_select"
                )
                if not trend_metric: trend_metric = '열 생산량 (Gcal)'
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                # --- 연도별 비교 ---
                st.markdown("#### 📅 연도별 비교")
                c1, c2 = st.columns(2)
                with c1:
                    year1 = st.selectbox("비교 연도 1 (파란색)", years, key="comp_y1")
                with c2:
                    year2 = st.selectbox("비교 연도 2 (빨간색)", years, index=len(years)-1 if len(years)>1 else 0, key="comp_y2")
                    
                yearly_dict = {}
                for m in range(1, 13):
                    row_data = {}
                    for y, label in [(year1, f"{year1}년"), (year2, f"{year2}년")]:
                        m_df, _, _, _ = load_data('월별', year=y, month=m) # load_data 반환값 변경
                        if not m_df.empty:
                            heat = pd.to_numeric(m_df['열 생산량 (Gcal)'], errors='coerce').sum()
                            lng = pd.to_numeric(m_df['LNG 사용량 (m³)'], errors='coerce').sum()
                            ghg = pd.to_numeric(m_df['온실가스 배출량 (tCO₂)'], errors='coerce').sum()
                            row_data[f"{label} 열 생산량 (Gcal)"] = heat
                            row_data[f"{label} LNG 사용량 (m³)"] = lng
                            row_data[f"{label} 온실가스 배출량 (tCO₂)"] = ghg
                        else:
                            row_data[f"{label} 열 생산량 (Gcal)"] = 0.0
                            row_data[f"{label} LNG 사용량 (m³)"] = 0.0
                            row_data[f"{label} 온실가스 배출량 (tCO₂)"] = 0.0
                    yearly_dict[f"{m}월"] = row_data
                
                if yearly_dict:
                    yearly_df = pd.DataFrame.from_dict(yearly_dict, orient='index')
                    yearly_df.index = pd.Categorical(yearly_df.index, categories=list(yearly_dict.keys()), ordered=True)
                    
                    cols_to_plot = [f"{year1}년 {trend_metric}", f"{year2}년 {trend_metric}"]
                    # --- ECharts를 이용한 3D 느낌의 막대 그래프 렌더링 ---
                    y1_data = [float(v) if pd.notna(v) else 0.0 for v in yearly_df[cols_to_plot[0]]]
                    y2_data = [float(v) if pd.notna(v) else 0.0 for v in yearly_df[cols_to_plot[1]]]
                    categories = list(yearly_df.index)
                    
                    html_code = f"""
                        <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
                        <div id="chart-container" style="height: 400px; width: 100%;"></div>
                        <script>
                        var chartDom = document.getElementById('chart-container');
                        var myChart = echarts.init(chartDom);
                        var option = {{
                            tooltip: {{ // 툴팁 디자인 개선
                                trigger: 'axis',
                                axisPointer: {{ type: 'shadow', shadowStyle: {{ color: 'rgba(0, 0, 0, 0.05)' }} }},
                                backgroundColor: 'rgba(255, 255, 255, 0.95)',
                                borderColor: '#edf2f7',
                                borderWidth: 1,
                                padding: [10, 15],
                                textStyle: {{ color: '#2d3748', fontSize: 13 }},
                                formatter: function (params) {{
                                    let result = '<div style="font-weight:bold; margin-bottom:5px; padding-bottom:5px; border-bottom:1px solid #edf2f7;">' + params[0].name + '</div>';
                                    params.forEach(function (item) {{
                                        result += '<div style="display:flex; justify-content:space-between; align-items:center; margin-top:3px;">' +
                                                  '<span style="font-size:12px; color:#4a5568;">' + item.marker + item.seriesName + '&nbsp;&nbsp;</span>' + 
                                                  '<span style="font-weight:600; color:#2d3748;">' + parseFloat(item.value).toLocaleString(undefined, {{minimumFractionDigits: 1, maximumFractionDigits: 1}}) + '</span>' +
                                                  '</div>';
                                    }});
                                    return result;
                                }}
                            }},
                            legend: {{ data: ['{year1}년', '{year2}년'], bottom: 0, icon: 'circle', textStyle: {{ color: '#4a5568', fontWeight: '600', fontSize: 13 }} }},
                            grid: {{ left: '3%', right: '4%', bottom: '12%', top: '15%', containLabel: true }},
                            xAxis: [{{ // X축 디자인 개선
                                type: 'category',
                                data: {categories},
                                axisLine: {{ show: false }},
                                axisTick: {{ show: false }},
                                axisLabel: {{ color: '#718096', margin: 12, fontWeight: '500' }}
                            }}],
                            yAxis: [{{ // Y축 디자인 개선
                                type: 'value',
                                name: '{trend_metric}',
                                nameTextStyle: {{ color: '#718096', padding: [0, 0, 0, 10], fontWeight: '500' }},
                                splitLine: {{ lineStyle: {{ type: 'dashed', color: '#e2e8f0' }} }},
                                axisLabel: {{ color: '#718096' }}
                            }}],
                            series: [
                                {{
                                    name: '{year1}년',
                                    type: 'bar',
                                    barGap: '15%', // 막대 간격 조정
                                    barMaxWidth: 40, // 막대 최대 너비
                                    itemStyle: {{
                                        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                                            {{ offset: 0, color: '#36D1DC' }},
                                            {{ offset: 1, color: '#5B86E5' }}
                                        ]),
                                        borderRadius: [6, 6, 0, 0],
                                        shadowColor: 'rgba(91, 134, 229, 0.3)',
                                        shadowBlur: 10,
                                        shadowOffsetX: 0,
                                        shadowOffsetY: 4
                                    }},
                                    data: {y1_data}
                                }},
                                {{
                                    name: '{year2}년',
                                    type: 'bar',
                                    barMaxWidth: 40, // 막대 최대 너비
                                    itemStyle: {{
                                        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                                            {{ offset: 0, color: '#FF416C' }},
                                            {{ offset: 1, color: '#FF4B2B' }}
                                        ]),
                                        borderRadius: [6, 6, 0, 0],
                                        shadowColor: 'rgba(255, 75, 43, 0.3)',
                                        shadowBlur: 10,
                                        shadowOffsetX: 0,
                                        shadowOffsetY: 4
                                    }},
                                    data: {y2_data}
                                }}
                            ]
                        }};
                        myChart.setOption(option);
                        window.addEventListener('resize', function() {{ myChart.resize(); }});
                    </script>
                    """
                    components.html(html_code, height=450)
                else:
                    st.info("연도별 비교 데이터가 없습니다.")

            elif analysis_target == "외부수열":
                all_facilities = ['남부소각장', 'ERG', 'SRF', '인천종합에너지', '안산도시개발']
                
                selected_facs_comp = st.segmented_control(
                    "수열처 선택 (선택하지 않으면 전체 누적 표시)",
                    all_facilities,
                    default=[],
                    selection_mode="multi",
                    key="comp_fac_select_heat"
                )
                
                facilities_to_plot = selected_facs_comp if selected_facs_comp else all_facilities

                st.markdown("<br>", unsafe_allow_html=True)
                st.markdown("#### 📅 연도별 비교")
                c1_h, c2_h = st.columns(2)
                with c1_h:
                    year1_h = st.selectbox("비교 연도 1 (파란색)", years, key="comp_y1_heat")
                with c2_h:
                    year2_h = st.selectbox("비교 연도 2 (빨간색)", years, index=len(years)-1 if len(years)>1 else 0, key="comp_y2_heat")
                    
                yearly_dict_heat = {}
                for m in range(1, 13):
                    row_data = {}
                    for y, label in [(year1_h, f"{year1_h}년"), (year2_h, f"{year2_h}년")]:
                        _, _, eh_summary, _ = load_data('월별', year=y, month=m)
                        for fac in facilities_to_plot:
                            val = eh_summary.get(fac, 0) if eh_summary else 0
                            row_data[f"{label} {fac}"] = val
                    yearly_dict_heat[f"{m}월"] = row_data
                    
                if yearly_dict_heat:
                    yearly_df_heat = pd.DataFrame.from_dict(yearly_dict_heat, orient='index')
                    categories = list(yearly_df_heat.index)
                    
                    colors_y1 = {'남부소각장': '#3498db', 'ERG': '#2980b9', 'SRF': '#1abc9c', '인천종합에너지': '#16a085', '안산도시개발': '#9b59b6'}
                    colors_y2 = {'남부소각장': '#e74c3c', 'ERG': '#c0392b', 'SRF': '#e67e22', '인천종합에너지': '#d35400', '안산도시개발': '#f1c40f'}
                    
                    series_data = []
                    legend_data = []
                    for fac in facilities_to_plot:
                        y1_data = [float(v) if pd.notna(v) else 0.0 for v in yearly_df_heat[f"{year1_h}년 {fac}"]]
                        y2_data = [float(v) if pd.notna(v) else 0.0 for v in yearly_df_heat[f"{year2_h}년 {fac}"]]
                        
                        series_data.append(f"""{{
                            name: '{fac} ({year1_h}년)',
                            type: 'bar',
                            stack: '{year1_h}년',
                            barMaxWidth: 40,
                            itemStyle: {{ color: '{colors_y1.get(fac, '#333')}' }},
                            data: {y1_data}
                        }}""")
                        series_data.append(f"""{{
                            name: '{fac} ({year2_h}년)',
                            type: 'bar',
                            stack: '{year2_h}년',
                            barMaxWidth: 40,
                            itemStyle: {{ color: '{colors_y2.get(fac, '#999')}' }},
                            data: {y2_data}
                        }}""")
                        legend_data.append(f"'{fac} ({year1_h}년)'")
                        legend_data.append(f"'{fac} ({year2_h}년)'")
                        
                    series_str = ",\n".join(series_data)
                    legend_str = ",\n".join(legend_data)
                    
                    html_code = f"""
                    <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
                    <div id="chart-container-heat-comp" style="height: 400px; width: 100%;"></div>
                    <script>
                    var chartDom = document.getElementById('chart-container-heat-comp');
                    var myChart = echarts.init(chartDom);
                    var option = {{
                        tooltip: {{
                            trigger: 'axis',
                            axisPointer: {{ type: 'shadow', shadowStyle: {{ color: 'rgba(0, 0, 0, 0.05)' }} }},
                            backgroundColor: 'rgba(255, 255, 255, 0.95)',
                            borderColor: '#edf2f7',
                            borderWidth: 1,
                            padding: [10, 15],
                            textStyle: {{ color: '#2d3748', fontSize: 13 }},
                            formatter: function (params) {{
                                let result = '<div style="font-weight:bold; margin-bottom:5px; padding-bottom:5px; border-bottom:1px solid #edf2f7;">' + params[0].name + '</div>';
                                params.forEach(function (item) {{
                                    if(parseFloat(item.value) > 0) {{
                                        result += '<div style="display:flex; justify-content:space-between; align-items:center; margin-top:3px;">' +
                                                  '<span style="font-size:12px; color:#4a5568;">' + item.marker + item.seriesName + '&nbsp;&nbsp;</span>' + 
                                                  '<span style="font-weight:600; color:#2d3748;">' + parseFloat(item.value).toLocaleString(undefined, {{minimumFractionDigits: 1, maximumFractionDigits: 1}}) + '</span>' +
                                                  '</div>';
                                    }}
                                }});
                                return result;
                            }}
                        }},
                        legend: {{ 
                            data: [{legend_str}], 
                            bottom: 0, 
                            type: 'scroll',
                            icon: 'circle', 
                            textStyle: {{ color: '#4a5568', fontWeight: '600', fontSize: 11 }} 
                        }},
                        grid: {{ left: '3%', right: '4%', bottom: '15%', top: '15%', containLabel: true }},
                        xAxis: [{{ 
                            type: 'category', 
                            data: {categories}, 
                            axisLine: {{ show: false }},
                            axisTick: {{ show: false }},
                            axisLabel: {{ color: '#718096', margin: 12, fontWeight: '500' }}
                        }}],
                        yAxis: [{{ 
                            type: 'value', 
                            name: '수열량 (Gcal)',
                            nameTextStyle: {{ color: '#718096', padding: [0, 0, 0, 10], fontWeight: '500' }},
                            splitLine: {{ lineStyle: {{ type: 'dashed', color: '#e2e8f0' }} }},
                            axisLabel: {{ color: '#718096' }}
                        }}],
                        series: [{series_str}]
                    }};
                    myChart.setOption(option);
                    window.addEventListener('resize', function() {{ myChart.resize(); }});
                    </script>
                    """
                    components.html(html_code, height=450)
                else:
                    st.info("연도별 비교 데이터가 없습니다.")

            elif analysis_target == "폐수배출시설":
                wastewater_items_cumulative = {
                    "총 상수도 사용량": "m³", "순수 생산량": "m³", "방류수량": "m³",
                    "폐수전력 사용량": "kWh", "생활용수량": "m³", "1차 냉각수량": "m³"
                }
                wastewater_metrics = list(wastewater_items_cumulative.keys())
                
                trend_metric_water = st.segmented_control(
                    "그래프 지표 선택", 
                    wastewater_metrics, 
                    default=wastewater_metrics[0], 
                    selection_mode="single",
                    key="comp_metric_select_water"
                )
                if not trend_metric_water: trend_metric_water = wastewater_metrics[0]
                
                st.markdown("<br>", unsafe_allow_html=True)

                # --- 연도별 비교 (폐수) ---
                st.markdown("#### 📅 연도별 비교")
                c1_w, c2_w = st.columns(2)
                with c1_w:
                    year1_w = st.selectbox("비교 연도 1 (녹색)", years, key="comp_y1_water")
                with c2_w:
                    year2_w = st.selectbox("비교 연도 2 (분홍색)", years, index=len(years)-1 if len(years)>1 else 0, key="comp_y2_water")
                    
                yearly_dict_water = {}
                for m in range(1, 13):
                    row_data = {}
                    for y, label in [(year1_w, f"{year1_w}년"), (year2_w, f"{year2_w}년")]:
                        _, summary, _, _ = load_data('월별', year=y, month=m) # load_data 반환값 변경
                        val = summary.get(trend_metric_water, 0) if summary else 0
                        row_data[f"{label} {trend_metric_water}"] = val
                    yearly_dict_water[f"{m}월"] = row_data
                    
                if yearly_dict_water:
                    yearly_df_water = pd.DataFrame.from_dict(yearly_dict_water, orient='index')
                    
                    cols_to_plot = [f"{year1_w}년 {trend_metric_water}", f"{year2_w}년 {trend_metric_water}"]
                    y1_data = [float(v) if pd.notna(v) else 0.0 for v in yearly_df_water[cols_to_plot[0]]]
                    y2_data = [float(v) if pd.notna(v) else 0.0 for v in yearly_df_water[cols_to_plot[1]]]
                    categories = list(yearly_df_water.index)
                    
                    html_code = f"""
                    <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
                    <div id="chart-container-water" style="height: 400px; width: 100%;"></div>
                    <script>
                    var chartDom = document.getElementById('chart-container-water');
                    var myChart = echarts.init(chartDom);
                    var option = {{
                        tooltip: {{
                            trigger: 'axis',
                            axisPointer: {{ type: 'shadow', shadowStyle: {{ color: 'rgba(0, 0, 0, 0.05)' }} }},
                            backgroundColor: 'rgba(255, 255, 255, 0.95)',
                            borderColor: '#edf2f7',
                            borderWidth: 1,
                            padding: [10, 15],
                            textStyle: {{ color: '#2d3748', fontSize: 13 }},
                            formatter: function (params) {{
                                let result = '<div style="font-weight:bold; margin-bottom:5px; padding-bottom:5px; border-bottom:1px solid #edf2f7;">' + params[0].name + '</div>';
                                params.forEach(function (item) {{
                                    result += '<div style="display:flex; justify-content:space-between; align-items:center; margin-top:3px;">' +
                                              '<span style="font-size:12px; color:#4a5568;">' + item.marker + item.seriesName + '&nbsp;&nbsp;</span>' + 
                                              '<span style="font-weight:600; color:#2d3748;">' + parseFloat(item.value).toLocaleString(undefined, {{minimumFractionDigits: 1, maximumFractionDigits: 1}}) + '</span>' +
                                              '</div>';
                                }});
                                return result;
                            }}
                        }},
                        legend: {{ data: ['{year1_w}년', '{year2_w}년'], bottom: 0, icon: 'circle', textStyle: {{ color: '#4a5568', fontWeight: '600', fontSize: 13 }} }},
                        grid: {{ left: '3%', right: '4%', bottom: '12%', top: '15%', containLabel: true }},
                        xAxis: [{{ 
                            type: 'category', 
                            data: {categories}, 
                            axisLine: {{ show: false }},
                            axisTick: {{ show: false }},
                            axisLabel: {{ color: '#718096', margin: 12, fontWeight: '500' }}
                        }}],
                        yAxis: [{{ 
                            type: 'value', 
                            name: '{trend_metric_water}',
                            nameTextStyle: {{ color: '#718096', padding: [0, 0, 0, 10], fontWeight: '500' }},
                            splitLine: {{ lineStyle: {{ type: 'dashed', color: '#e2e8f0' }} }},
                            axisLabel: {{ color: '#718096' }}
                        }}],
                        series: [
                            {{
                                name: '{year1_w}년',
                                type: 'bar',
                                barGap: '15%',
                                barMaxWidth: 40,
                                itemStyle: {{
                                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                                        {{ offset: 0, color: '#00b09b' }},
                                        {{ offset: 1, color: '#96c93d' }}
                                    ]),
                                    borderRadius: [6, 6, 0, 0],
                                    shadowColor: 'rgba(150, 201, 61, 0.3)',
                                    shadowBlur: 10,
                                    shadowOffsetX: 0,
                                    shadowOffsetY: 4
                                }},
                                data: {y1_data}
                            }},
                            {{
                                name: '{year2_w}년',
                                type: 'bar',
                                barMaxWidth: 40,
                                itemStyle: {{
                                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                                        {{ offset: 0, color: '#DA22FF' }},
                                        {{ offset: 1, color: '#9733EE' }}
                                    ]),
                                    borderRadius: [6, 6, 0, 0],
                                    shadowColor: 'rgba(151, 51, 238, 0.3)',
                                    shadowBlur: 10,
                                    shadowOffsetX: 0,
                                    shadowOffsetY: 4
                                }},
                                data: {y2_data}
                            }}
                        ]
                    }};
                    myChart.setOption(option);
                    window.addEventListener('resize', function() {{ myChart.resize(); }});
                    </script>
                    """
                    components.html(html_code, height=450)
                else:
                    st.info("연도별 비교 데이터가 없습니다.")