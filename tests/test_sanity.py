from pathlib import Path

gdef test_election_module_exists():
    assert Path("src/election.py").exists()