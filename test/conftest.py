# content of conftest.py
import pytest

def pytest_addoption(parser):
    parser.addoption("--dataDir", action = "store", default = "/data/",
        help = "Data directory")

@pytest.fixture
def dataDir(request):
    return request.config.getoption("--dataDir")