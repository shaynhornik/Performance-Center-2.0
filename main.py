import os
from app import create_app

app = create_app()
app.config["DEBUG"] = True
app.config["PROPAGATE_EXCEPTIONS"] = True

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
