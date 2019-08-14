from __future__ import absolute_import
from distutils.core import setup


long_description = '''
Blingalytics
============

Blingalytics is an open-source Python project for building a flexible,
powerful business reporting system.

* Define new reports quickly by writing simple Python

* Pluggable data source and caching structure - use the built-in modules or
  write one for your own particular needs

* Output your report data to powerful HTML/JavaScript tables, with paging,
  sorting and more

For the full documentation, please visit it over on `Read the Docs`_. Or if
you're feeling particularly do-it-yourself today, you can always build the
docs yourself with Sphinx_.

Thanks to Adly_ for fostering the development of Blingalytics as an
open-source project. Blingalytics is released under the `MIT License`_.

.. _Read the Docs: http://blingalytics.readthedocs.org/
.. _Sphinx: http://sphinx.pocoo.org/
.. _Adly: http://adly.com/
.. _MIT License: http://www.opensource.org/licenses/mit-license
'''

setup(
    name='Blingalytics',
    version='0.1.3',
    author='Jeff Schenck',
    author_email='jmschenck@gmail.com',
    url='http://github.com/jeffschenck/blingalytics',
    description='Blingalytics is a tool for building reports from your data.',
    long_description=long_description,
    license='',
    packages=[
        'blingalytics',
        'blingalytics.caches',
        'blingalytics.sources',
        'blingalytics.utils',
        'blingalytics.statics',
        'blingalytics.statics.css',
        'blingalytics.statics.js',
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: MIT License',
        'Topic :: Office/Business',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Scientific/Engineering',
    ],
    install_requires=['future==0.16.0'],
)
