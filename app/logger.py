import logging

# Configure logging for the application
# The basicConfig sets up the default handler so that debug messages are written to the console.
# A logger specific to this module (__name__) is then created for more granular control if needed.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
