from distutils.core import setup


readme = open('README.rst').read()

setup(
    name='Blingalytics',
    version='0.1dev',
    author='Jeff Schenck, Adly Inc.',
    author_email='jmschenck@gmail.com',
    url='http://github.com/adly/blingalytics',
    description='Blingalytics is a tool for building reports from your data.',
    long_description=readme.decode('utf-8'),
    license='',
    packages=[
        'blingalytics',
        'blingalytics.caches',
        'blingalytics.sources',
        'blingalytics.utils',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Programming Language :: Python',
        'License :: OSI Approved :: MIT License',
        'Topic :: Office/Business',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Scientific/Engineering',
    ],
)
