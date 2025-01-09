import sys
import os
from config.config_loader import Config

def load_config():
    current_dir = os.path.dirname(__file__)
    parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
    sys.path.append(parent_dir)
    return Config()