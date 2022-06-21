import logging

from app import create_app

from dotenv import load_dotenv


app = create_app()


logging.getLogger().setLevel(logging.INFO)
if __name__ == "__main__":
    load_dotenv()  # This is done because `app.py` is only run in development environments
    app.run(host="0.0.0.0", port=8080)
