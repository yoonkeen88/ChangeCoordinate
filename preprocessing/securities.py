import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 환경 변수 가져오기
api_key = os.getenv("KAKAO_API_KEY")

if api_key:
    print(f"API Key Loaded: {api_key[:5]}******")  # 보안상 일부만 출력
else:
    print("API Key not found!")
