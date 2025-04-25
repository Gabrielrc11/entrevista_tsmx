import pandas as pd
import logging
import os
from datetime import datetime

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f'importacao_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
)