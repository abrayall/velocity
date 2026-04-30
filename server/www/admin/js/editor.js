// Content Editor module - form and raw JSON editing
var Editor = (function() {
    var currentMode = 'form'; // 'form' or 'raw'
    var currentData = {};
    var originalData = {};
    var onSaveCallback = null;

    function render(container, data, onSave, onCancel) {
        currentData = data || {};
        originalData = JSON.parse(JSON.stringify(currentData));
        onSaveCallback = onSave;
        currentMode = 'form';

        container.innerHTML = '<div class="editor-container">' +
            '<div class="editor-toolbar">' +
                '<div class="editor-tabs">' +
                    '<button class="editor-tab active" data-mode="form">Form</button>' +
                    '<button class="editor-tab" data-mode="raw">Raw JSON</button>' +
                '</div>' +
            '</div>' +
            '<div class="editor-body" id="editorBody"></div>' +
            '<div class="editor-footer">' +
                '<button class="btn btn-primary btn-sm" id="editorSave">Save</button>' +
                '<button class="btn btn-ghost btn-sm" id="editorCancel">Cancel</button>' +
            '</div>' +
        '</div>';

        // Tab switching
        container.querySelectorAll('.editor-tab').forEach(function(tab) {
            tab.addEventListener('click', function() {
                syncFromView(container);
                currentMode = this.dataset.mode;
                container.querySelectorAll('.editor-tab').forEach(function(t) { t.classList.remove('active'); });
                this.classList.add('active');
                renderView(container.querySelector('#editorBody'));
            });
        });

        // Save
        container.querySelector('#editorSave').addEventListener('click', function() {
            syncFromView(container);
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

        renderView(container.querySelector('#editorBody'));
    }

    function renderView(body) {
        if (currentMode === 'raw') {
            renderRaw(body);
        } else {
            renderForm(body);
        }
    }

    function renderForm(body) {
        var keys = Object.keys(currentData);
        if (keys.length === 0) {
            // Empty - show a single key/value input to start
            body.innerHTML = '<div class="form-fields">' +
                '<div class="form-group">' +
                    '<label>Key</label>' +
                    '<input type="text" class="field-key" placeholder="Enter field name">' +
                '</div>' +
                '<div class="form-group">' +
                    '<label>Value</label>' +
                    '<textarea class="field-value" rows="3" placeholder="Enter value"></textarea>' +
                '</div>' +
                '<button class="btn btn-ghost btn-sm" id="addFieldBtn">+ Add Field</button>' +
            '</div>';
        } else {
            var html = '<div class="form-fields">';
            keys.forEach(function(key) {
                var val = currentData[key];
                var displayVal = typeof val === 'object' ? JSON.stringify(val, null, 2) : String(val);
                var isLong = displayVal.length > 80 || typeof val === 'object';
                html += '<div class="form-group" data-key="' + escapeHtml(key) + '">' +
                    '<label>' + escapeHtml(key) + '</label>';
                if (isLong) {
                    html += '<textarea class="field-value" rows="4">' + escapeHtml(displayVal) + '</textarea>';
                } else {
                    html += '<input type="text" class="field-value" value="' + escapeAttr(displayVal) + '">';
                }
                html += '</div>';
            });
            html += '<button class="btn btn-ghost btn-sm" id="addFieldBtn">+ Add Field</button>';
            html += '</div>';
            body.innerHTML = html;
        }

        var addBtn = body.querySelector('#addFieldBtn');
        if (addBtn) {
            addBtn.addEventListener('click', function() {
                var group = document.createElement('div');
                group.className = 'form-group';
                group.innerHTML = '<input type="text" class="field-key" placeholder="Field name" style="margin-bottom:6px;">' +
                    '<input type="text" class="field-value" placeholder="Value">';
                this.parentNode.insertBefore(group, this);
            });
        }
    }

    function renderRaw(body) {
        var json = JSON.stringify(currentData, null, 2);
        body.innerHTML = '<textarea class="raw-editor" id="rawEditor">' + escapeHtml(json) + '</textarea>';
    }

    function syncFromView(container) {
        if (currentMode === 'raw') {
            var rawEl = container.querySelector('#rawEditor');
            if (rawEl) {
                try {
                    currentData = JSON.parse(rawEl.value);
                } catch (e) {
                    // Keep current data on parse error
                }
            }
        } else {
            var newData = {};
            container.querySelectorAll('.form-group[data-key]').forEach(function(group) {
                var key = group.dataset.key;
                var input = group.querySelector('.field-value');
                if (input) {
                    var val = input.value || input.textContent;
                    try {
                        newData[key] = JSON.parse(val);
                    } catch (e) {
                        newData[key] = val;
                    }
                }
            });
            // New fields
            container.querySelectorAll('.form-group:not([data-key])').forEach(function(group) {
                var keyInput = group.querySelector('.field-key');
                var valInput = group.querySelector('.field-value');
                if (keyInput && valInput && keyInput.value.trim()) {
                    var val = valInput.value;
                    try {
                        newData[keyInput.value.trim()] = JSON.parse(val);
                    } catch (e) {
                        newData[keyInput.value.trim()] = val;
                    }
                }
            });
            if (Object.keys(newData).length > 0) {
                currentData = newData;
            }
        }
    }

    function getData(container) {
        syncFromView(container);
        return currentData;
    }

    function escapeHtml(str) {
        var div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    function escapeAttr(str) {
        return str.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    return {
        render: render,
        getData: getData
    };
})();
