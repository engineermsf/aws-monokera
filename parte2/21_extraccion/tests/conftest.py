import sys
from pathlib import Path

# Permite importar extraction al ejecutar pytest desde la raíz del repo
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
