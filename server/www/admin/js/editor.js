// Content Editor module - raw JSON text editing
var Editor = (function() {
    var currentData = {};
    var onSaveCallback = null;

    function render(container, data, onSave, onCancel) {
        currentData = data || {};
        onSaveCallback = onSave;

        var json = JSON.stringify(currentData, null, 2);

        container.innerHTML = '<div class="editor-container">' +
            '<div class="editor-toolbar">' +
                '<div class="editor-tabs">' +
                    '<span class="editor-tab active" style="cursor:default;">JSON</span>' +
                '</div>' +
            '</div>' +
            '<div class="editor-body">' +
                '<textarea class="raw-editor" id="rawEditor">' + escapeHtml(json) + '</textarea>' +
            '</div>' +
            '<div class="editor-footer">' +
                '<button class="btn btn-primary btn-sm" id="editorSave">Save</button>' +
                '<button class="btn btn-ghost btn-sm" id="editorCancel">Cancel</button>' +
            '</div>' +
        '</div>';

        // Save
        container.querySelector('#editorSave').addEventListener('click', function() {
            var rawEl = container.querySelector('#rawEditor');
            if (rawEl) {
                try {
                    currentData = JSON.parse(rawEl.value);
                } catch (e) {
                    // Keep current data on parse error
                }
            }
            if (onSaveCallback) onSaveCallback(currentData);
        });

        // Cancel
        container.querySelector('#editorCancel').addEventListener('click', function() {
            if (onCancel) {
                onCancel();
            } else {
                window.history.back();
            }
        });
    }

    function getData(container) {
        var rawEl = container.querySelector('#rawEditor');
        if (rawEl) {
            try {
                currentData = JSON.parse(rawEl.value);
            } catch (e) {
                // Keep current data on parse error
            }
        }
        return currentData;
    }

    function escapeHtml(str) {
        var div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    return {
        render: render,
        getData: getData
    };
})();
