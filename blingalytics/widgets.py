"""
Widgets provide a mechanism for displaying basic HTML inputs to the user
and then cleaning the input and passing it into a report.

All widgets should derive from the base Widget class. See that class's
documentation for more information.
"""

from datetime import date, datetime, timedelta


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
    
    By default, widgets accept the following parameters as keyword arguments
    when being instantiated in report definitions:
    
    * label: The label the user sees for this widget. Optional; defaults to
      'Filter'.
    * default: A default value that is initially displayed in the widget. 
      Optional; defaults to None.
    * required: Whether the input can be left blank. If True, a blank value
      will raise a ValidationError; if False (the default), a blank value will
      be returned as None.
    * extra_class: A string or iterable of strings that will add extra HTML
      classes to the widget. Optional.
    * extra_attrs: A dict of attribute name to attribute value that will be
      rendered into the HTML form element. Optional.
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
        Renders the widget to HTML. Subclasses must implement this themselves.
        """
        raise NotImplementedError

class Checkbox(Widget):
    """
    A checkbox user input widget.
    
    Default will be evaluated as checked if it is truthy, or unchecked
    if it is falsy. Default can be a callable.
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
    Produces a text box for a datepicker widget interface.
    
    * date_format: This is the string passed to the datetime.strptime function
      to attempt to convert the textual user input into a datetime object.
      Optional, defaults to '%m/%d/%Y'.
    
    This widget is rendered as an HTML text input, and will have a class of
    'bl_datepicker'. It is left up to the JavaScript to add the appropriate
    datepicker interface desired.
    
    Default can be a string in the format specified by the date_format option,
    a date or datetime object, or it can be one of these special strings:
    'today', 'yesterday', 'first_of_month'. The default can also be a
    callable, in which case it is called and then evaluated as described
    above.
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
        try:
            return datetime.strptime(user_input, self.date_format)
        except (ValueError, TypeError):
            raise ValidationError('Date is not in the correct format.')

class Select(Widget):
    """
    Produces an HTML select for user input.
    
    * choices: A list of two-tuples representing the select options to
    display. The first value in each tuple should be the "cleaned" value to be
    returned to the report; the second value in the tuple should be the label
    to be displayed to the user. Optional, defaults to (). Can be a callable.
    
    This widget's default option uses Python-like indexing into the choices
    list to determine the default selection.
    
    For example, if you have three select options, you can make the second
    option default by passing in default=1. If you want the last selection to
    be default, you can pass in default=-1. Default may also be a callable,
    which will be evaluated before applying the above logic.
    """
    def __init__(self, choices=(), **kwargs):
        self.choices = choices
        self._widget_class = 'bl_select'
        super(Select, self).__init__(**kwargs)

    def render(self):
        value = self.default() if callable(self.default) else self.default
        self.choices = self.choices() if callable(self.choices) else self.choices
        options = ''
        for i, (choice_value, choice_label) in enumerate(self.choices):
            # Handle positive/negative indexing for default value
            selected = ''
            if value is not None:
                if value >= 0 and value == i:
                    selected = 'selected'
                elif value < 0 and len(self.choices) + value == i:
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
        self.choices = callable(self.choices) and self.choices() or self.choices
        return self.choices[i][0]

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
            return [int(id.strip()) for id in user_input.split(' ')]
        except (ValueError, AttributeError):
            raise ValidationError('Could not convert input to list of IDs.')

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
    Produces a text box for an autocomplete widget.
    
    * default: Unlike most widgets, the default argument is not supported
      for autocomplete.
    * multiple: Sets whether this autocomplete should accept just one or
      multiple inputs. Defaults to False.
    
    This widget is rendered as an HTML text input, and will have a class of
    'bl_autocomplete'. It will also have a class of 'bl_multiple' if it allows
    multiple inputs. It is left up to the JavaScript to add the appropriate
    autocompletion interface.
    
    The cleaned user input will be a list of integers.
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
            ids = [int(id.strip()) for id in user_input.split(' ')]
            if len(ids) > 1 and not self.multiple:
                raise ValidationError('Multiple selections not allowed.')
            return ids
        except (ValueError, AttributeError):
            raise ValidationError('Could not convert input to list of IDs.')
