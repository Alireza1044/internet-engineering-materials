from flask import Flask, send_file, request, make_response

app = Flask(__name__)


@app.route('/1.jpeg')
def return_first():
    return send_file('statics/1.jpeg')


if __name__ == '__main__':
    app.run(port=5025)
