Frontend Implementation
=======================

The most common use case for Blingalytics is to be displayed on a web page,
so we wanted to provide a pre-baked solution that can get you up and running
in minutes. You're welcome to tweak it or even roll your own, but this is a
great starting point.

In the HTML
-----------

To implement the Blingalytics frontend on your site, the first thing you'll
need to do is include the appropriate CSS and JavaScript files on the page.
These static files are included under ``blingalytics/statics/css/`` and
``blingalytics/statics/js/`` and should be made available by your server.

CSS to include:

.. code-block:: html

    <link rel="stylesheet" href="/static/css/blingalytics.css" type="text/css" />

JavaScript to include:

.. code-block:: html

    <script src="//ajax.googleapis.com/ajax/libs/jquery/1.6.2/jquery.min.js"></script>
    <script src="/static/js/jquery.dataTables.min.js"></script>
    <script src="/static/js/jquery.blingalytics.js"></script>

Once you've included the static dependencies on the page, you can use the
blingalytics jQuery plugin to insert a report table anywhere on your page:

.. code-block:: html

    <script>
        jQuery('#selector').blingalytics({'reportCodeName': 'report_code_name'});
    </script>

For now the plugin only accepts two options:

``reportCodeName`` *(optional)*
    The :class:`report class <blingalytics.base.Report>` ``code_name``
    attribute. This specifies which report should be displayed on the page.
    Defaults to ``'report'``.

``url`` *(optional)*
    The URL to hit when contacting the server for an AJAX response. Defaults
    to ``'/report/'``.

On the backend
--------------

The blingalytics jQuery plugin inserts a bunch of HTML and JavaScript that
talks over AJAX with your server. So your server, at the ``url`` you specify
in the plugin options, should respond appropriately. To make this easy, a
Python helper function is provided.

.. autofunction:: blingalytics.helpers.report_response
