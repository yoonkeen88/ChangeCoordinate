import requests
import time
import pandas as pd
import os
import numpy as np
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# API 키 목록
api_keys = [
    os.getenv("KAKAO_API_KEY_1"),
    os.getenv("KAKAO_API_KEY_2"),
    os.getenv("KAKAO_API_KEY_3")
]
current_api_index = 0  # 현재 API 키 인덱스

PROGRESS_FILE = "progress.txt"  # 진행 상태 저장 파일


# API 요청 함수 (주소 → 좌표 변환)
def get_lat_lon(address):
    global current_api_index

    url = "https://dapi.kakao.com/v2/local/search/address.json"

    for _ in range(3):  # 최대 3번 재시도
        headers = {"Authorization": f"KakaoAK {api_keys[current_api_index]}"}
        params = {"query": address}

        try:
            response = requests.get(url, headers=headers, params=params, timeout=5)

            # 요청 초과 (Rate Limit)
            if response.status_code == 429:
                print(f"🚨 요청 제한! API 변경 → {current_api_index + 1}/{len(api_keys)}")
                
                # API 키 변경
                current_api_index = (current_api_index + 1) % len(api_keys)
                
                print(f"✅ 새로운 API 키 적용: {current_api_index + 1}/{len(api_keys)}")
                time.sleep(10)  # 10초 대기 후 재시도
                continue  

            if response.status_code == 200:
                result = response.json()
                if result['documents']:
                    lat = result['documents'][0]['y']
                    lon = result['documents'][0]['x']
                    return float(lat), float(lon)

        except requests.exceptions.ConnectionError as e:
            print(f"❌ 연결 오류: {e}, 5초 대기 후 재시도")
            time.sleep(5)

        except requests.exceptions.RequestException as e:
            print(f"❌ API 요청 오류: {e}")

    return None, None  # 좌표 변환 실패 시


# 변환 실행 함수
def process_dataframe(df_part, part_num):
    file_name = f"좌표변환_{part_num}.csv"
    
    # 기존에 변환된 파일이 있으면 건너뛰기
    if os.path.exists(file_name):
        print(f"✅ {file_name} 이미 존재, 건너뜀!")
        return
    
    df_part = df_part.copy()

    # 좌표 변환 적용
    df_part["위도"], df_part["경도"] = zip(*df_part["전체주소"].apply(get_lat_lon))

    # 좌표 변환이 실패한 행 저장
    failed_df = df_part[df_part["위도"].isna()]
    if not failed_df.empty:
        failed_df.to_csv(f"failed_addresses_{part_num}.csv", mode="a", header=False, index=False, encoding="utf-8-sig")
        print(f"⚠️ 변환 실패한 {len(failed_df)}개 주소 저장됨 (failed_addresses_{part_num}.csv)")

    # 변환된 데이터 저장
    df_part.to_csv(file_name, index=False, encoding="utf-8-sig")
    print(f"✅ {file_name} 처리 완료!")

# 마지막으로 처리한 파트 번호 가져오기
def get_last_processed_part():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return int(f.read().strip())
    return 0  # 파일이 없으면 처음부터 시작


# 마지막으로 처리한 파트 번호 저장하기
def save_progress(part_num):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(part_num))


def main():
    # CSV 파일 읽기 (경로 수정)
    # file_path = "/Users/angwang-yun/Desktop/Project/coor/초등학교_도로명_전체주소.csv"
    df = pd.read_csv("/Users/angwang-yun/Desktop/Project/coor/초등학교_도로명_전체주소.csv")
    # df = pd.read_csv(file_path)

    # 전체 데이터 크기
    total_rows = len(df)
    
    # 10만 개씩 나누기
    chunk_size = 5000
    df_chunks = [df[i:i + chunk_size] for i in range(0, total_rows, chunk_size)]

    # 마지막으로 처리한 부분 불러오기
    last_part = get_last_processed_part()
    
    # 변환 실행
    for i, df_part in enumerate(df_chunks, start=1):
        if i <= last_part:  # 이미 처리한 부분은 건너뛰기
            continue

        processed_rows = len(df_part)
        progress = (sum(len(df_chunks[j]) for j in range(i)) / total_rows) * 100  # 진행률 계산
        
        print(f"🔹 {i}번째 데이터셋 처리 시작... ({processed_rows} 행)")
        print(f"🚀 진행률: {progress:.2f}% 완료")

        process_dataframe(df_part, i)

        # 진행 상태 저장
        save_progress(i)

        time.sleep(60)  # API 제한 대비 1분 대기


if __name__ == "__main__":
    main()
