Development
===========

Get the code
------------

Fork us on Github! You can find us at `https://github.com/adly/blingalytics`_. 

Contributing
------------

There are a few ways you can get involved with the development of
Blingalytics.

Use Blingalytics!
^^^^^^^^^^^^^^^^^

Use the code to build your business' reporting system and let us know how it
goes. We want to know how you're using it, what problems you encountered, and
what features you'd love to see.

Report bugs
^^^^^^^^^^^

If you think you've found a bug in the code, please check our ticket tracker
to see if anyone else has reported it yet. If not, file a bug report! Standard
issue reporting standards apply: please succinctly describe the steps to
reproduce and what the issue is. For now, our issues are on our
`Github issues`_ page.

Contribute patches and new features
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you've fixed a bug or have a feature that you think would be useful to the
community, send us a pull request on Github.

Planned features
----------------

A list, in pseudo-random order, of some features we're planning. Some of these
are already in use at Adly and just need some polish before they're ready to
be included. Others are brand-new features the community would benefit from.

* **Front-end implementation.** Internally we use an AJAXy front-end
  implementation built on top of YUI's DataTable. We'd like to make this
  generic enough that it's a snap to get your own project up and running with
  it.

* **Django app.** Having a quick and easy app to plug into your Django_ project
  would be super. This would involve a new source to interface with Django's
  ORM, perhaps a new cache interface to Django's caching framework, and
  probably some other niceties.

* **More included sources.** While sources are pluggable enough that you can write
  one for your own purposes, there are some common sources that we'd like to
  support. We've considered adding sources that pull from a MapReduce, from
  a public web API, and from various other datastores like MongoDB. 

* **Memcache cache implementation.** We use Redis extensively at Adly, and it
  works incredibly well for caching and crunching report data. But I expect
  people are more familiar with Memcache and more likely to already have it
  set up in their infrastructure. It probably won't be as memory-efficient,
  but it should be possible to create an implementation if there's enough of
  a use case.

* **Others?** If you have other features you think would be useful, please mention
  them on the mailing list!

.. _https://github.com/adly/blingalytics: https://github.com/adly/blingalytics
.. _Github issues: https://github.com/adly/blingalytics/issues
.. _Django: https://www.djangoproject.com/
