import setuptools

setuptools.setup(
    name="seikaoroshi",
    version="0.0.1",
    packages=setuptools.find_packages(),
    install_requires=["requests>=2.24.0", "beautifulsoup4>=4.9.0", "pandas>=1.1.2",],
    python_requires='>=3.7',
)