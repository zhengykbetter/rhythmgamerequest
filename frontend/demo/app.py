from flask import Flask
import random

app = Flask(__name__)

@app.route('/')
def index():
    # 完整网页代码
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>RhythmGameQuery</title>
        <style>
            body { text-align: center; margin-top: 50px; font-size: 20px; }
            button { padding: 10px 20px; font-size: 18px; cursor: pointer; margin: 20px; }
        </style>
    </head>
    <body>
        <h1>✅ 我的网站跑通啦！</h1>
        <p>如果你是Potassium，CSNCSN！</p>
        <button onclick="checkLuck()">今日运势</button>
        <p id="result"></p>

        <script>
        function checkLuck() {
            // 80%概率100分，20%概率10分
            let rand = Math.random();
            let res = rand < 0.8 ? "恭喜你，你的运势是100分" : "恭喜你，你的运势是10分";
            document.getElementById("result").innerText = res;
        }
        </script>
    </body>
    </html>
    '''
    return html

if __name__ == '__main__':
    app.run()
