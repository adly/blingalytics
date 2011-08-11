(function($) {

  jQuery.fn.blingalytics = function(options) {
    var settings = {
      'url': '/report/',
      'reportCodeName': 'report'
    };
    if (options) $.extend(settings, options);

    return this.each(function() {
      var container = $(this);
      var params = {metadata: '1', report: settings.reportCodeName};
      $.getJSON(settings.url, params, function(metadata) {
        var headers = metadata.header;
        var defaultSort = metadata.default_sort;
        var columns = [];
        var classes = [];
        var thead = '';
        for (var i in headers) {
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
          if (headers[i].key == defaultSort[0]) {
            defaultSort = ([parseInt(i), defaultSort[1]]);
          }
        }

        // Construct the skeleton of the table
        var table = $(
          '<table class="bl_table display"><thead>' +
          '<tr>' + thead + '</tr>' +
          '</thead><tbody></tbody></table>'
        );
        container.empty().append(table);
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
          iDeferLoading: 100,
          iDisplayLength: 25,
          fnRowCallback: function(nRow, aData, iDisplayIndex) {
            var children = nRow.children;
            for (var i in children) {
              children[i].className += classes[i];
            }
            return nRow;
          }
        });
        datatable.fnSort([defaultSort]);
      });
    });
  };

})(jQuery);
