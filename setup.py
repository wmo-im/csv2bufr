from distutils.core import setup
setup(
  name = 'csv2bufr',
  packages = ['csv2bufr'],
  version = '0.1.0',
  license='Apache Software License',
  description = 'Configurable module to convert data from CSV to WMO BUFR format',
  author = 'David I. Berry',
  author_email = 'DBerry@wmo.int',
  url = 'https://github.com/wmo-im/CSV2BUFR',
  download_url = 'https://github.com/wmo-im/CSV2BUFR/archive/v_01.tar.gz',
  keywords = ['WMO', 'BUFR', 'csv', 'encoding', 'weather', 'observations'],
  install_requires=[
          'eccodes',
          'jsonschema',
          'StringIO',
          'BytesIO',
          'hashlib',
          'logging',
          'csv',
          'pytest'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Science/Research',
    'Topic :: Scientific/Engineering',
    'License :: OSI Approved :: Apache Software License',   # Again, pick a license
    'Programming Language :: Python :: 3.10'      #Specify which python versions that you want to support
  ],
)