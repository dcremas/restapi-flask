import connexion

app = connexion.App(__name__, specification_dir="swagger/")
app.add_api("my_api.yml")


@app.route("/")
def home():
    return "You made it to the homepage!!"


if __name__ == "__main__":
    app.run()
