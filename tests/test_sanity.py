from pathlib import Path


def test_election_module_exists():
    assert Path("src/election.py").exists()