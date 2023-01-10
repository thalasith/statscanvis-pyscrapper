from flask import Flask
import os
app = Flask(__name__)

test = "Hi there"

@app.route("/")
def home():
    return "Hi there!"

if __name__ == '__main__':
    app.run(debug=True, port=os.getenv("PORT", default=5000))