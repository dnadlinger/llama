from setuptools import find_packages, setup

setup(
    name='llama',
    version='0.0.1',
    url='https://github.com/klickverbot/llama',
    author='David P. Nadlinger',
    packages=['llama'],
    install_requires=[
        'aiohttp',
        'sipyco'
    ]
)
