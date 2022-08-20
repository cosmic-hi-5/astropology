from setuptools import setup, find_packages

with open("README.md", "r") as file:
    long_description = file.read()

setup(
    name="astropology",
    version="0.1.0",
    author="Blanco Agustina, Graham Mathew, Jaramillo Diego, Ortiz Edgar, Xiangyu Jin",
    author_email="ed.ortizm@gmail.com",
    packages=find_packages(where="src", include=["[a-z]*"], exclude=[]),
    package_dir={"": "src"},
    description="Python for anomaly detection in time series data with persistence homology",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cosmic-hi-5/astropology",
    license="MIT",
    keywords="Time series, TDA, Astronomy",
)
