import sys

from setuptools import setup, find_packages

exec(open('bbgbridge/version.py').read())


def check_python_version():
    if sys.version_info[:2] < (3, 4):
        print('Python 3.4 or newer is required. Python version detected: {}'.format(sys.version_info))
        sys.exit(-1)


def main():
    setup(name='bbgbridge',
          version=__version__,
          author='Ran Fan, Yu Zheng, ArrayStream Technologies',
          author_email='info@arraystream.com',
          url='https://github.com/ran404/bbgbridge',
          description='Easy to use Bloomberg Desktop API wrapper in Python',
          long_description='Easy to use Bloomberg Desktop API wrapper in Python',
          classifiers=[
              'Development Status :: 4 - Beta',
              'Programming Language :: Python :: 3',
              'Programming Language :: Python :: 3.4',
              'Programming Language :: Python :: 3.5',
              'Intended Audience :: Financial and Insurance Industry'
          ],
          license='LPGL',
          packages=find_packages(include=['bbgbridge']),
          install_requires=['pandas'],
          platforms='any')


if __name__ == '__main__':
    check_python_version()
    main()
