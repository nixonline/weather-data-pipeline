from dotenv import load_dotenv
import os

load_dotenv()

def main():
    print("Running extractor...")
    print("Bucket:", os.getenv("GCS_BUCKET"))

if __name__ == "__main__":
    main()