from setuptools import setup


DEPS = [ ]

setup(
    name='livingpark_utils',
    version='0.2',
    description='Utility functions to write LivingPark notebooks.',
    author='Tristan Glatard',
    author_email='tristan.glatard@concordia.ca',
    license='MIT',
    packages=['livingpark_utils'],
    setup_requires=DEPS,
    install_requires=DEPS,
)
