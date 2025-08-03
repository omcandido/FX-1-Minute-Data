from fx_logging import get_project_logger

# Setup logging
logger = get_project_logger(__name__)

def main():
    logger.info("Hello from fx-1-minute-data!")


if __name__ == "__main__":
    main()
