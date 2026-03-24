from flask import Flask, send_from_directory

app = Flask(__name__, 
            static_folder='frontend',
            static_url_path='')

# 首页：加载现代化前端
@app.route('/')
def index():
    return send_from_directory('frontend', 'index.html')

# 子页面支持
@app.route('/pages/<name>')
def page(name):
    return send_from_directory('frontend/pages', f'{name}.html')

if __name__ == '__main__':
    app.run()
