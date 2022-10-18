from setuptools import setup, find_packages

MAIN_MODULE = '__init__'

# Find the agent package that contains the main module
packages = find_packages('.')
agent_package = 'ace_skyspark'

# Find the version number from the main module
agent_module = agent_package
_temp = __import__(agent_module, globals(), locals(), ['__version__'], 0)
__version__ = _temp.__version__
print(packages)

# Setup
setup(
    name='ace_skyspark',
    version=__version__,
    author="Andrew Rodgers",
    author_email="andrew@aceiotsolutions.com",
    url="https://aceiotsolutions.com",
    description="ACE SkySpark API interface",
    install_requires=['requests', 'scramp'],
    packages=packages,
    # entry_points={
    #     'setuptools.installation': [
    #         'eggsecutable = ' + agent_module + ':main',
    #     ]
    # }
)