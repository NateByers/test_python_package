import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='test_python_package',
    version='0.0.1',
    author='Nathan Byers',
    author_email='nbyers@coh.org',
    description='Testing installation of Package',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='http://git.coh.org:7990/users/nbyers/repos/test_python_package',
    project_urls = {
        "Bug Tracker": "https://github.com/NateByers/test_python_package/issues"
    },
    license='MIT',
    packages=['test_python_package'],
    install_requires=['os']
)