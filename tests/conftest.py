"""
pytest configuration — add project root to sys.path for all tests.
"""
import sys
import os

# Ensure the project root is on the import path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
