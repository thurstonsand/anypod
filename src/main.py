from config import AppSettings


def main():
    print("Loading Anypod configuration...")
    try:
        # Instantiate AppSettings; config_file will be sourced from
        # init args (none here), env vars (CONFIG_FILE), or the field default.
        settings = AppSettings()  # type: ignore[call-arg]
        # model_dump_json is convenient for a direct JSON string
        settings_json = settings.model_dump_json(indent=2)
        print("\nLoaded Configuration:")
        print(settings_json)

        # Example of how to access a specific feed's setting if needed:
        # if settings.feeds:
        #     first_feed_name = list(settings.feeds.keys())[0]
        #     first_feed_config = settings.feeds[first_feed_name]
        #     print(f"\nExample: URL for feed '{first_feed_name}': {first_feed_config.url}")

    except Exception as e:
        print(f"Error loading or processing configuration: {e}")
        print(
            "Please ensure your configuration file (e.g., /config/feeds.yaml or via CONFIG_FILE env var) is correctly set up."
        )


if __name__ == "__main__":
    main()
