"""Setup configuration for the Query Orchestrator package."""

from setuptools import setup, find_packages

setup(
    name="query_orchestrator",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "langchain>=0.1.0",
        "langgraph>=0.0.20",
        "langchain-openai>=0.0.5",
        "pymongo>=4.6.1",
        "sqlalchemy>=2.0.25",
        "python-dotenv>=1.0.0",
        "pydantic>=2.5.3",
        "pydantic-settings>=2.1.0",
        "typing-extensions>=4.9.0",
        "pytest>=7.4.4",
        "google-api-python-client>=2.108.0",
        "google-auth-httplib2>=0.1.1",
        "google-auth-oauthlib>=1.1.0",
        "python-jose>=3.3.0",
        "aiohttp>=3.9.1",
        "asyncio>=3.4.3",
        "rich>=13.7.0",
        "loguru>=0.7.2",
    ],
    python_requires=">=3.9",
    author="Your Name",
    author_email="your.email@example.com",
    description="A sophisticated system for natural language interaction with multiple databases",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
) 