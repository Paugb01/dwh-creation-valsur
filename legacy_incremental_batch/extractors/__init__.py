"""
MySQL to GCP Data Extractors Package
"""

from .base_extractor import BaseExtractor

try:
    from .simple_extractor import SimpleExtractor
except ImportError:
    SimpleExtractor = None

try:
    from .incremental_extractor import IncrementalExtractor
except ImportError:
    IncrementalExtractor = None

try:
    from .batch_extractor import BatchExtractor
except ImportError:
    BatchExtractor = None

__all__ = [
    'BaseExtractor',
    'SimpleExtractor', 
    'IncrementalExtractor',
    'BatchExtractor'
]

__version__ = "2.0.0"
