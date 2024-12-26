import logging

def setup_logger(name):
    """
    Set up a logger with the specified name.

    :param name: Name of the logger.
    :return: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levellevel)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    return logger
