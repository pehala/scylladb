import pytest


@pytest.fixture(scope="function")
def space_limited_directory(pytestconfig):
    space_directory = pytestconfig.getoption("space_limited_dir")
    print("Checking if space directory is set")
    if not space_directory:
        pytest.skip("Space limited directory not available")
    yield space_directory
    print("Cleaning up")
    # shutil.rmtree(space_directory)

