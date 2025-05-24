from app import create_app

app = create_app()

if __name__ == "__main__":
    # Replit starts this for you, but keeps local dev happy
    app.run(host="0.0.0.0", port=3000, debug=True)
