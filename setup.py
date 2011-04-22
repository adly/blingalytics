from distutils.core import setup


setup(
    name='Blingalytics',
    version='0.1dev',
    author='Jeff Schenck, Adly Inc.',
    author_email='jeff@adly.com',
    url='http://blingalytics.com/',
    description='Blingalytics is a tool for building reports from your data.',
    license='',
    packages=[
        'blingalytics',
        'blingalytics.caches',
        'blingalytics.sources',
        'blingalytics.utils',
    ],
)
