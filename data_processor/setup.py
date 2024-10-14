import setuptools

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

__version__ = "0.4.0"

REPO_NAME = 'data_processor'
AUTHOR_USER_NAME = 'Krutika Tekwani'
SRC_REPO = 'data_processor'
AUTHOR_EMAIL = "mayank.aroa@curriebrown.com"

setuptools.setup(
    name=SRC_REPO,
    version=__version__,
    author=AUTHOR_USER_NAME,
    author_email=AUTHOR_EMAIL,
    description="streamline operations on a datasets",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="GPL-3.0",
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "Development Status :: 2 - Pre-Alpha",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)"],

    url=f"https://github.com/{AUTHOR_USER_NAME}/{REPO_NAME}",
    project_urls={
        "Bug Tracker": f"https://github.com/{AUTHOR_USER_NAME}/{REPO_NAME}/issues",
    },
    packages=setuptools.find_packages(exclude=['tests', '*.tests', '*.tests.*']),
    include_package_data=True,
    install_requires=[
        "PyYAML==6.0.2",
        "pytest==7.4.4",
        "python-box==6.0.2",
        "typing_extensions==4.12.1"
    ],
)