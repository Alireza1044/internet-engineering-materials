from flask import Flask, jsonify, url_for, request, redirect, make_response, send_file, render_template

app = Flask(__name__, template_folder='templates')


@app.route("/no-cors", methods=['GET', 'POST'])
def index():
    return render_template("no-cors.html")


@app.route("/cors", methods=['GET', 'POST'])
def gist():
    return render_template("cors.html")


if __name__ == '__main__':
    app.run(port=5000)
