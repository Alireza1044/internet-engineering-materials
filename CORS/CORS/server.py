from flask import Flask, send_file, request, make_response

app = Flask(__name__)


@app.route('/1.jpeg')
def return_first():
    return send_file('statics/1.jpeg')


@app.after_request
def after_request_func(response):
    origin = request.headers.get('Origin')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    response.set_cookie('session_cookie', "moradi-kabiri")
    if origin:
        response.headers.add('Access-Control-Allow-Origin', origin)
    return response


if __name__ == '__main__':
    app.run(port=5005)
