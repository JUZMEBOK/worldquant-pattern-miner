import requests
import json
import time
from pathlib import Path
from urllib.parse import urljoin

PROJECT_DIR = Path(__file__).resolve().parent.parent
CREDENTIALS_DIR = PROJECT_DIR / "credentials"
PW_FILE = CREDENTIALS_DIR / "pw"
TOKEN_FILE = CREDENTIALS_DIR / "brain_token.txt"


def authenticate_with_persona(auto_poll_interval=5, max_retries=30):
    session = requests.Session()

    # Load credentials securely once
    with PW_FILE.open() as f:
        session.auth = tuple(json.load(f))

    # Initial authentication attempt
    response = session.post("https://api.worldquantbrain.com/authentication")

    # Detect biometric authentication requirement
    if response.status_code == 401 and response.headers.get("WWW-Authenticate") == "persona":
        biometric_url = urljoin(response.url, response.headers["Location"])

        print("Persona Biometric Verification is required.")
        print(f"Please open the following URL in your browser and complete the verification:")
        print(biometric_url)

        print("\nWaiting for biometric verification to complete...")

        # Automatically retry authentication request every few seconds
        for retry in range(max_retries):
            print(f"Attempt {retry + 1}/{max_retries}...")
            response = session.post(biometric_url)

            if response.status_code == 201:
                print("Persona verification completed successfully!")
                break  # Successfully authenticated
            else:
                print("Verification still pending, retrying shortly...")
                time.sleep(auto_poll_interval)
        else:  # triggered if the loop finishes without successful authentication
            print("Max retries reached - Verification failed or was not completed in time.")
            exit(1)

    # Final status check
    if response.status_code != 201:
        print(f"Authentication failed: {response.status_code} - {response.text}")
        exit(1)

    print("Authenticated successfully!")

    # Capture and save JWT token
    jwt_token = session.cookies.get("t")
    CREDENTIALS_DIR.mkdir(parents=True, exist_ok=True)
    with TOKEN_FILE.open("w") as f:
        f.write(jwt_token)
    print(f"Authentication token stored at {TOKEN_FILE}.")

    # Double-check the authentication session
    session_check = session.get("https://api.worldquantbrain.com/authentication")
    if session_check.status_code != 200:
        print(f"Session validation failed: {session_check.status_code}. Please try again.")
        exit(1)

    print("Session verified and is active!")


if __name__ == "__main__":
    authenticate_with_persona()
