from setuptools import setup, find_packages

setup(
    name="utils",
    version="0.1",
    zip_safe=False,
    packages=find_packages(),
    package_data={
        # If any package contains *.txt files, include them:
        '': ['*.txt'],
    },
    include_package_data=True
)