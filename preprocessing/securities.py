import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 여러 API 키 가져오기
api_key_1 = os.getenv("KAKAO_API_KEY_1")
api_key_2 = os.getenv("KAKAO_API_KEY_2")
api_key_3 = os.getenv("KAKAO_API_KEY_3")


# 키가 존재하면 일부 출력
if api_key_1:
    print(f"API Key 1 Loaded: {api_key_1[:5]}******")  # 보안상 일부만 출력
else:
    print("API Key 1 not found!")

if api_key_2:
    print(f"API Key 2 Loaded: {api_key_2[:5]}******")  # 보안상 일부만 출력
else:
    print("API Key 2 not found!")
if api_key_3:
    print(f"API Key 3 Loaded: {api_key_3[:5]}******")  # 보안상 일부만 출력
else:
    print("API Key 3 not found!")