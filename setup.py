from setuptools import setup

setup(name='sciwonc.dataflow',
      version='0.1',
      description='Interface for data management into scientific workflows management systems on clouds',
      url='http://github.com/storborg/funniest',
      author='Elaine Naomi Watanabe',
      author_email='elainew@ime.usp.br',
      license='MIT',
      packages=['sciwonc.dataflow'],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
        "pymongo",
        "psycopg2"
    ])
