from flask import render_template
import connexion

app = connexion.App(__name__, specification_dir="swagger/")
app.add_api("my_api.yml")


@app.route("/")
def home():
    return render_template("home.html")


if __name__ == "__main__":
    app.run()
