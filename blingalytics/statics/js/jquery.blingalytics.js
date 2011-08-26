(function($) {

  jQuery.fn.blingalytics = function(options) {
    var settings = {
      'url': '/report/',
      'reportCodeName': 'report'
    };
    if (options) $.extend(settings, options);

    return this.each(function() {
      var container = $(this);
      container.empty().addClass('bl_container');
      var errors_ul = $('<ul class="errors"></ul>');
      errors_ul.prependTo(container);
      var params = {metadata: '1', report: settings.reportCodeName};
      $.getJSON(settings.url, params, function(metadata) {
        if (metadata.errors.length) {
          errors_ul.empty();
          for (var i = 0; i < metadata.errors.length; i++) {
            errors_ul.append('<li>' + metadata.errors[i] + '</li>');
          }
          return;
        }
        var headers = metadata.header;
        var defaultSort = metadata.default_sort;
        var columns = [];
        var classes = [];
        var thead = '';
        var tfoot = '';
        for (var i = 0; i < headers.length; i++) {
          columns.push({
            'bSearchable': false,
            'bSortable': headers[i].sortable,
            'sClass': 'hello',
            'sName': headers[i].key,
            'sTitle': headers[i].label,
            'aTargets': [i]
          });
          var colClasses = headers[i].className || '';
          colClasses = headers[i].hidden ? colClasses + ' bl_hidden' : colClasses;
          classes.push(colClasses);
          thead += '<th class="' + colClasses + '">' + headers[i].label + '</th>';
          tfoot += '<th class="' + colClasses + '"></th>';
          if (headers[i].key == defaultSort[0]) {
            defaultSort = ([parseInt(i), defaultSort[1]]);
          }
        }

        // Construct the widgets
        var widgets_container = $('<ul class="widgets"></ul>');
        container.append(widgets_container);
        for (var i = 0; i < metadata.widgets.length; i++) {
          widgets_container.append('<li>' + metadata.widgets[i] + '</li>');
        }
        $('<button>Run Report</button>').appendTo(widgets_container).click(function() {
          datatable.fnSort([defaultSort]);
        });

        // Construct the skeleton of the table
        var table = $(
          '<table class="bl_table display">' +
          '<thead><tr>' + thead + '</tr></thead>' +
          '<tfoot><tr>' + tfoot + '</tr></tfoot>' +
          '<tbody></tbody>' +
          '</table>'
        );
        container.append(table);

        // Init the datatable widget
        var datatable = table.dataTable({
          aoColumnDefs: columns,
          sPaginationType: 'full_numbers',
          bAutoWidth: false,
          bFilter: false,
          bJQueryUI: true,
          bProcessing: true,
          bStateSave: true,
          bServerSide: true,
          sAjaxSource: settings.url + '?report=' + settings.reportCodeName,
          aaSorting: [defaultSort],
          iDeferLoading: 0,
          iDisplayLength: 25,
          fnRowCallback: function(nRow, aData, iDisplayIndex) {
            var children = nRow.children;
            for (var i = 0; i < children.length; i++) {
              children[i].className += classes[i];
            }
            return nRow;
          },
          fnServerData: function(sSource, aoData, fnCallback) {
            var url = sSource + '&' + widgets_container.find('input, select, textarea').serialize();
            $.getJSON(url, aoData, function(data) {
              if (data.errors.length) {
                errors_ul.empty();
                for (var i = 0; i < data.errors.length; i++) {
                  errors_ul.append('<li>' + data.errors[i] + '</li>');
                }
                fnCallback({
                  'iTotalRecords': 0,
                  'iTotalDisplayRecords': 0,
                  'aaData': []
                });
              } else {
                var footer_tds = table.find('tfoot th');
                for (var i = 0; i < data.footer; i++) {
                  footer_tds.eq(i).html(data.footer[i]);
                }
                fnCallback(data);
              }
            });
          },
          fnHeaderCallback: function(nHead, aasData, iStart, iEnd, aiDisplay) {
            // Fix missing header sorting classes
            var head = $(nHead);
            head.find('th').removeClass('sorting');
            head.find('th:has(.ui-icon-triangle-1-n, .ui-icon-triangle-1-s)').addClass('sorting');
          }
        });
      });
    });
  };

})(jQuery);
