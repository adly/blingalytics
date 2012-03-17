"""
Widgets provide a mechanism for displaying basic HTML inputs to the user and
then cleaning the input and passing it into a report.

All widgets accept the following parameters as keyword arguments. Some widgets
may accept or require additional arguments, which will be specified in their
documentation.

* ``label``: The label the user sees for this widget. Defaults to
  ``'Filter'``.
* ``default``: A default value that is initially displayed in the widget. This
  can be a callable, which will be evaluated lazily when rendering the widget.
  Defaults to ``None``.
* ``required``: Whether the input can be left blank. If ``True``, a blank
  value will be added to the errors produced by the report's
  :meth:`clean_user_inputs <blingalytics.base.Report.clean_user_inputs>`
  method. By default, user input is not required, and a blank value will be
  returned as None.
* ``extra_class``: A string or list of strings that will add extra HTML
  classes to the rendered widget. Defaults to no extra classes.
* ``extra_attrs``: A dict of attribute names to values. These will be added as
  attributes on the rendered widget. Defaults to no extra attributes.
"""

from datetime import date, datetime, timedelta
import re


INPUT = '''
  <label for="%(form_name)s">%(form_label)s</label>
  <input id="%(form_name)s" name="%(form_name)s" class="%(form_class)s" type="%(form_type)s" %(form_attrs)s />
'''.strip()
SELECT = '''
  <label for="%(form_name)s">%(form_label)s</label>
  <select id="%(form_name)s" name="%(form_name)s" class="%(form_class)s" %(form_attrs)s>%(form_options)s</select>
'''.strip()
SELECT_OPTION = '''
  <option value="%(form_value)s" %(form_selected)s>%(form_label)s</option>
'''.strip()

class ValidationError(Exception):
    pass

class Widget(object):
    """
    Base widget implementation.

    All widgets should derive from this class. Generally, a widget class will
    define its own clean and render methods, as well as whatever related
    functionality it requires.
    """
    def __init__(self, label=None, default=None, required=False, extra_class=None, extra_attrs=None):
        self.label = label if label is not None else 'Filter'
        self.default = default
        self.required = required
        self._extra_attrs = extra_attrs
        if isinstance(extra_class, basestring):
            self.extra_class = (extra_class,)
        else:
            self.extra_class = extra_class

    def get_unique_id(self, dirty_inputs):
        user_input = dirty_inputs.get(self.form_name)
        if not user_input:
            user_input = dirty_inputs.get(self._name, '')
        return '%s|%s' % (self.form_name, user_input)

    @property
    def form_name(self):
        """Returns the form name to be used in HTML."""
        return '%s_%s' % (self._report_code_name, self._name)

    def _form_class(self, widget_class):
        if self.extra_class:
            return widget_class + ' ' + ' '.join(self.extra_class)
        return widget_class

    @property
    def extra_attrs(self):
        if self._extra_attrs:
            return ' '.join(['%s="%s"' % (k, self._extra_attrs[k]) for k in self._extra_attrs])
        return ''

    def get_choices(self):
        raise NotImplementedError('Not available for this type of widget.')

    def clean(self, user_input):
        """
        Basic user input cleaning example. Subclasses will generally override
        this clean method to implement their own validation and parsing of the
        HTML form input.

        The clean method of a subclass needs to handle the required option,
        either by calling this method using super or implementing its own
        version. If the widget is required, an empty user input should raise a
        ValidationError; if the widget is not required, an empty user input
        should be returned as None.
        """
        if user_input in (None, ''):
            if self.required:
                raise ValidationError('%s is required.' % self.label)
            else:
                return None
        return user_input

    def render(self):
        """
        Renders the widget to HTML. Default implementation is to render a text input.
        """
        value = self.default() if callable(self.default) else self.default
        return INPUT % {
            'form_name': self.form_name,
            'form_label': self.label,
            'form_class': self._form_class('bl_input'),
            'form_type': 'text',
            'form_attrs': 'value="%s" %s' % (value if value is not None else '', self.extra_attrs),
        }

class Checkbox(Widget):
    """
    Produces a checkbox user input widget. The widget's default value will be
    evaluated as checked if it's truthy and unchecked if it's falsy.
    """
    def render(self):
        value = self.default() if callable(self.default) else self.default
        return INPUT % {
            'form_name': self.form_name,
            'form_label': self.label,
            'form_class': self._form_class('bl_checkbox'),
            'form_type': 'checkbox',
            'form_attrs': '%s %s' % (('checked' if value else ''), self.extra_attrs),
        }

    def clean(self, user_input):
        """Transforms the user input to a boolean."""
        user_input = super(Checkbox, self).clean(user_input)
        return bool(user_input)

class DatePicker(Widget):
    """
    Produces a text input to build into a datepicker widget. It accepts one
    additional optional argument:

    * ``date_format``: This is the string passed to the ``datetime.strptime``
      function to convert the textual user input into a datetime object.
      Defaults to ``'%m/%d/%Y'``.

    Note that this widget is rendered simply as an HTML text input with a
    class of ``'bl_datepicker'``. It is left up to you to throw a JavaScript
    datepicker widget on top of it with jQuery or whatever.

    For this widget, the ``default`` argument can be:

    * A string in the format specified by the ``date_format`` option.
    * A ``date`` or ``datetime`` object.
    * One of the following special strings: ``'today'``, ``'yesterday'``,
      ``'first_of_month'``.
    * A callable that evaluates to any of the previous options.
    """
    def __init__(self, date_format='%m/%d/%Y', **kwargs):
        self.date_format = date_format
        super(DatePicker, self).__init__(**kwargs)

    def render(self):
        value = self.default() if callable(self.default) else self.default
        if value == 'today':
            value = datetime.utcnow().strftime(self.date_format)
        elif value == 'yesterday':
            value = (datetime.utcnow() - timedelta(days=1)).strftime(self.date_format)
        elif value == 'first_of_month':
            value = datetime.utcnow().replace(day=1).strftime(self.date_format)
        elif isinstance(value, basestring):
            value = datetime.strptime(value, self.date_format).strftime(self.date_format)
        elif isinstance(value, (date, datetime)):
            value = value.strftime(self.date_format)
        else:
            value = ''
        return INPUT % {
            'form_name': self.form_name,
            'form_label': self.label,
            'form_class': self._form_class('bl_datepicker'),
            'form_type': 'text',
            'form_attrs': 'value="%s" %s' % (value, self.extra_attrs),
        }

    def clean(self, user_input):
        """Validates the date and converts to datetime object."""
        user_input = super(DatePicker, self).clean(user_input)
        if user_input:
            try:
                return datetime.strptime(user_input, self.date_format)
            except (ValueError, TypeError):
                raise ValidationError('Date is not in the correct format.')

class Select(Widget):
    """
    Produces an select input widget. This takes one additional optional
    argument:

    * ``choices``: A list of two-tuples representing the select options to
      display. The first item in each tuple should be the "cleaned" value
      that will be returned when the user selects this option. The second item
      should be the label to be displayed to the user for this option. This
      can also be a callable. Defaults to ``[]``, an empty list of choices.

    For the ``default`` argument for this type of widget, you provide an index
    into the choices list, similar to how you index into a Python list. For
    example, if you have three select options, you can default to the second
    option by passing in ``default=1``. If you want the last selection to
    be default, you can pass in ``default=-1``.
    """
    def __init__(self, choices=[], **kwargs):
        self.choices = choices
        self._widget_class = 'bl_select'
        super(Select, self).__init__(**kwargs)

    def get_unique_id(self, dirty_inputs):
        '''
        Generates a unique id for the widget.
        '''
        choices = self.get_choices()
        vals = ''
        for val in sorted(dict(choices).keys()):
            if re.search('[|:]', repr(val)):
                raise ValueError("%s widget choice values can't contain '|' or ':'. Provided: %s" % (self.label, repr(val)))
            vals += '%s,' % repr(val)

        user_input = dirty_inputs.get(self.form_name)
        if not user_input:
            user_input = dirty_inputs.get(self._name, '')
        return '%s|%s|%s' % (self.form_name, user_input, vals)

    def get_choices(self):
        return self.choices() if callable(self.choices) else self.choices

    def render(self):
        values = self.default() if callable(self.default) else self.default
        if not isinstance(values, (list, tuple)):
            values = [values]
        choices = self.get_choices()
        options = ''
        for i, (choice_value, choice_label) in enumerate(choices):
            # Handle positive/negative indexing for default value
            selected = ''
            if values is not None:
                for value in values:
                    if value is not None:
                        if value >= 0 and value == i:
                            selected = 'selected'
                        elif value < 0 and len(choices) + value == i:
                            selected = 'selected'
            options += SELECT_OPTION % {
                'form_value': i,
                'form_label': choice_label,
                'form_selected': selected,
            }
        return SELECT % {
            'form_options': options,
            'form_name': self.form_name,
            'form_label': self.label,
            'form_class': self._form_class(self._widget_class),
            'form_attrs': self.extra_attrs,
        }

    def clean(self, user_input):
        """Validates that a choice was selected and returns its value."""
        user_input = super(Select, self).clean(user_input)
        try:
            i = int(user_input)
        except (ValueError, TypeError):
            raise ValidationError('Please choose a valid option.')
        choices = self.get_choices()
        value = choices[i][0]
        return value if value != '' else None

class Multiselect(Select):
    """
    A mutliple selection widget.
    
    This widget provides a multiple select HTML widget. It will have a class
    of 'bl_multiselect' and an empty intial choices set. It is left to the
    frontend to fill in its choices set.
    """
    def __init__(self, **kwargs):
        super(Multiselect, self).__init__(**kwargs)
        self._widget_class = 'bl_multiselect'
        if self._extra_attrs:
            self._extra_attrs['multiple'] = 'multiple'
        else:
            self._extra_attrs = {'multiple': 'multiple'}

    def clean(self, user_input):
        """Validates a space-separated string of IDs into a list of ints."""
        if user_input in (None, ''):
            return None
        try:
            indexes = [int(id.strip()) for id in user_input.split(' ')]
        except (ValueError, AttributeError):
            raise ValidationError('Could not convert input to list of IDs.')

        choices = self.get_choices()
        return [choices[i][0] for i in indexes]

class TimezoneSelect(Select):
    """
    A timezone-specific Select widget.
    
    This widget simply provides a few default options for the standard Select
    widget. It adds a 'bl_timezone' class and defaults to 'Timezone' for the
    label.
    """
    def __init__(self, choices=(), **kwargs):
        kwargs.update({
            'extra_class': 'bl_timezone ' + kwargs.get('extra_class', ''),
            'choices': choices,
            'label': kwargs.get('label', 'Timezone'),
        })
        super(TimezoneSelect, self).__init__(**kwargs)

class Autocomplete(Widget):
    """
    Produces a text input for an autocomplete widget. Unlike most widgets,
    this **does not** accept the ``default`` argument. It takes the following
    extra argument:

    * ``multiple``: Whether the autocomplete should accept just one or
      multiple values. When set to ``True``, this will add a class of
      ``'bl_multiple'`` to the widget. Defaults to ``False``.

    Note that this widget is rendered simply as an HTML text input with a
    class of ``'bl_autocomplete'``, and optionally a class of
    ``'bl_multiple'``. It is left up to you to throw a JavaScript
    autocompletion widget on top of it with jQuery or whatever.

    The cleaned user input will be coerced to a list of integer IDs.
    """
    def __init__(self, multiple=False, **kwargs):
        self.multiple = multiple
        super(Autocomplete, self).__init__(**kwargs)

    def render(self):
        value = self.default() if callable(self.default) else self.default
        multiple = ' bl_multiple' if self.multiple else ''
        if value:
            raise ValueError('Autocomplete does not support default values.')
        return INPUT % {
            'form_name': self.form_name,
            'form_label': self.label,
            'form_class': self._form_class('bl_autocomplete%s' % multiple),
            'form_type': 'text',
            'form_attrs': 'value="" %s' % self.extra_attrs,
        }

    def clean(self, user_input):
        """Validates a space-separated string of IDs into a list of ints."""
        user_input = super(Autocomplete, self).clean(user_input)
        if user_input in (None, ''):
            return None
        try:
            ids = [int(id.strip()) for id in user_input.split(' ') if id]
            if len(ids) > 1 and not self.multiple:
                raise ValidationError('Multiple selections not allowed.')
            return ids
        except (ValueError, AttributeError):
            raise ValidationError('Could not convert input to list of IDs.')
