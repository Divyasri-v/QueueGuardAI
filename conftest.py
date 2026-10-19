"""
QueueGuard AI — pytest root conftest
Adds project root to sys.path so all modules are importable during tests.
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
