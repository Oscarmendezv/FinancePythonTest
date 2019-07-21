from setuptools import setup

setup(name='stock_project',
      version='0.1.0',
      packages=['stock_project'],
      entry_points={
          'console_scripts': [
              'stock_project = stock_project.src.__main__:main'
          ]
      },
      )