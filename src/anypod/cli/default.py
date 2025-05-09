from ..config import AppSettings  # Adjusted import path


def default():
    print("Loading Anypod configuration...")
    try:
        settings = AppSettings()  # type: ignore[call-arg]
        settings_json = settings.model_dump_json(indent=2)
        print("\nLoaded Configuration:")
        print(settings_json)
    except Exception as e:
        print(f"Error loading or processing configuration: {e}")
        print(
            "Please ensure your configuration file (e.g., /config/feeds.yaml or via CONFIG_FILE env var) is correctly set up."
        )
