from setuptools import setup, find_packages

setup(
    name="my_pyspark_project",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark==3.2.1",  # Add your dependencies here
    ],
    entry_points={
        "console_scripts": [
            "my_script=my_pyspark_project.module1:main_function",
        ],
    },
    author="Your Name",
    author_email="your.email@example.com",
    description="A description of your project",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/my_pyspark_project",  # Project URL
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
