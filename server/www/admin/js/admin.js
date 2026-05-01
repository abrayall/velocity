// Admin SPA - hash-based routing and views
var Admin = (function() {
    var currentTenant = '';
    var tenants = [];
    var contentTypes = [];
    var contentCounts = {};

    // Initialize
    async function init() {
        var valid = await Auth.checkSession();
        if (!valid) {
            window.location.href = '/admin/login';
            return;
        }

        await loadTenants();
        await loadContentTypes();
        setupLogout();

        window.addEventListener('hashchange', route);
        route();
    }

    async function loadTenants() {
        try {
            var resp = await fetch('/api/tenants');
            var data = await resp.json();
            tenants = data.tenants || [];
        } catch (e) {
            tenants = [];
        }

        var select = document.getElementById('tenantSelect');
        select.innerHTML = '';

        if (tenants.length === 0) {
            var opt = document.createElement('option');
            opt.value = 'demo';
            opt.textContent = 'demo';
            select.appendChild(opt);
            tenants = ['demo'];
        }

        tenants.forEach(function(t) {
            var opt = document.createElement('option');
            opt.value = t;
            opt.textContent = t;
            select.appendChild(opt);
        });

        // Restore saved tenant
        var savedTenant = localStorage.getItem('velocity_tenant');
        if (savedTenant && tenants.indexOf(savedTenant) !== -1) {
            select.value = savedTenant;
        }
        currentTenant = select.value;
        localStorage.setItem('velocity_tenant', currentTenant);

        select.addEventListener('change', async function() {
            currentTenant = this.value;
            localStorage.setItem('velocity_tenant', currentTenant);
            document.getElementById('viewContainer').innerHTML = '<div class="loading">Loading...</div>';
            await loadContentTypes();
            window.location.hash = '#/';
        });

        // Add tenant button
        var addBtn = document.getElementById('addTenantBtn');
        if (addBtn) {
            addBtn.addEventListener('click', function() {
                showCreateDialog('Create Tenant', 'Tenant name', async function(name) {
                    var resp = await fetch('/api/tenants', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ name: name })
                    });
                    if (!resp.ok) {
                        var err = await resp.json();
                        throw new Error(err.message || 'Failed to create tenant');
                    }
                    showToast('Tenant created', 'success');
                    await loadTenants();
                    select.value = name;
                    currentTenant = name;
                    localStorage.setItem('velocity_tenant', currentTenant);
                    await loadContentTypes();
                    route();
                });
            });
        }
    }

    async function loadContentTypes() {
        try {
            var resp = await fetch('/api/types', {
                headers: { 'X-Tenant': currentTenant }
            });
            var data = await resp.json();
            contentTypes = data.types || [];
        } catch (e) {
            contentTypes = [];
        }
        renderSidebar();
    }

    function renderSidebar() {
        var nav = document.getElementById('sidebarNav');
        var html = '';

        if (contentTypes.length > 0) {
            contentTypes.forEach(function(type) {
                html += '<a class="sidebar-link" href="#/content/' + type + '">' +
                    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path><polyline points="14 2 14 8 20 8"></polyline></svg>' +
                    escapeHtml(type) + '</a>';
            });
        } else {
            html += '<div class="sidebar-link text-muted" style="font-size:0.8rem;cursor:default;">No content types</div>';
        }

        html += '<a class="sidebar-link add-type-link" id="addTypeBtn" href="javascript:void(0)">' +
            '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"></line><line x1="5" y1="12" x2="19" y2="12"></line></svg>' +
            'Add Type</a>';

        nav.innerHTML = html;
        updateActiveSidebarLink();

        document.getElementById('addTypeBtn').addEventListener('click', function() {
            showCreateDialog('Create Content Type', 'Content type name', async function(name) {
                var resp = await fetch('/api/types', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Tenant': currentTenant
                    },
                    body: JSON.stringify({ name: name })
                });
                if (!resp.ok) {
                    var err = await resp.json();
                    throw new Error(err.message || 'Failed to create content type');
                }
                showToast('Content type created', 'success');
                await loadContentTypes();
                window.location.hash = '#/content/' + name;
            });
        });
    }

    function updateActiveSidebarLink() {
        var hash = window.location.hash || '#/';
        document.querySelectorAll('.sidebar-link').forEach(function(link) {
            link.classList.remove('active');
            var href = link.getAttribute('href');
            if (!href) return;
            if (href === hash) {
                link.classList.add('active');
            } else if (hash.startsWith(href + '/') && href !== '#/') {
                link.classList.add('active');
            }
        });
    }

    function setupLogout() {
        document.getElementById('logoutBtn').addEventListener('click', function(e) {
            e.preventDefault();
            Auth.logout();
        });
    }

    // Router
    function route() {
        var hash = window.location.hash || '#/';
        var view = document.getElementById('viewContainer');
        updateActiveSidebarLink();

        if (hash === '#/' || hash === '#') {
            if (contentTypes.length > 0) {
                window.location.hash = '#/content/' + contentTypes[0];
                return;
            }
            view.innerHTML = '<h1 class="view-title">No Content</h1><p class="text-muted">No content types found for this tenant.</p>';
        } else if (hash.match(/^#\/content\/([^/]+)\/new$/)) {
            var type = hash.match(/^#\/content\/([^/]+)\/new$/)[1];
            renderNewItem(view, type);
        } else if (hash.match(/^#\/content\/([^/]+)\/([^/]+)\/history$/)) {
            var m = hash.match(/^#\/content\/([^/]+)\/([^/]+)\/history$/);
            renderEditItem(view, m[1], m[2], { showHistory: true });
        } else if (hash.match(/^#\/content\/([^/]+)\/([^/]+)$/)) {
            var m = hash.match(/^#\/content\/([^/]+)\/([^/]+)$/);
            renderEditItem(view, m[1], m[2]);
        } else if (hash.match(/^#\/content\/([^/]+)$/)) {
            var type = hash.match(/^#\/content\/([^/]+)$/)[1];
            renderContentList(view, type);
        } else {
            view.innerHTML = '<h2>Not Found</h2><p class="text-muted">The page you are looking for does not exist.</p>';
        }
    }

    // Dashboard
    async function renderDashboard(container) {
        container.innerHTML = '<h1 class="view-title">Dashboard</h1>' +
            '<div class="dashboard-stats" id="dashStats"><div class="loading">Loading...</div></div>' +
            '<div style="margin-top:24px;">' +
                '<div class="table-container" id="dashContentTypes"></div>' +
            '</div>';

        // Load item counts per type
        var totalItems = 0;
        contentCounts = {};
        var promises = contentTypes.map(function(type) {
            return fetch('/api/content/' + encodeURIComponent(type), {
                headers: { 'X-Tenant': currentTenant }
            }).then(function(r) { return r.json(); }).then(function(data) {
                var count = (data.items || []).length;
                contentCounts[type] = count;
                totalItems += count;
            }).catch(function() {
                contentCounts[type] = 0;
            });
        });

        await Promise.all(promises);

        var stats = document.getElementById('dashStats');
        stats.innerHTML =
            '<div class="stat-card"><div class="stat-label">Content Types</div><div class="stat-value">' + contentTypes.length + '</div></div>' +
            '<div class="stat-card"><div class="stat-label">Total Items</div><div class="stat-value">' + totalItems + '</div></div>' +
            '<div class="stat-card"><div class="stat-label">Tenants</div><div class="stat-value">' + tenants.length + '</div></div>';

        // Content types table
        var tableEl = document.getElementById('dashContentTypes');
        if (contentTypes.length === 0) {
            tableEl.innerHTML = '<div class="table-empty">No content types found for this tenant.</div>';
            return;
        }

        var html = '<div class="table-header"><h3>Content Types</h3></div>' +
            '<table><thead><tr><th>Type</th><th>Items</th><th></th></tr></thead><tbody>';

        contentTypes.forEach(function(type) {
            var count = contentCounts[type] || 0;
            html += '<tr>' +
                '<td><a class="table-link" href="#/content/' + type + '">' + escapeHtml(type) + '</a></td>' +
                '<td class="text-muted text-sm">' + count + '</td>' +
                '<td class="table-actions"><a href="#/content/' + type + '" class="btn btn-ghost btn-sm">Browse</a></td>' +
                '</tr>';
        });

        html += '</tbody></table>';
        tableEl.innerHTML = html;
    }

    // Types / Schemas view
    async function renderTypes(container) {
        container.innerHTML = '<h1 class="view-title">Schemas</h1>' +
            '<p class="view-subtitle">Content type schemas for this tenant</p>' +
            '<div class="table-container" id="typesTable"><div class="loading">Loading...</div></div>';

        try {
            var resp = await fetch('/api/schemas');
            var globalData = await resp.json();
            var globalSchemas = globalData.schemas || [];

            var resp2 = await fetch('/api/tenant/schemas', {
                headers: { 'X-Tenant': currentTenant }
            });
            var tenantData = await resp2.json();
            var tenantSchemas = tenantData.schemas || [];

            var tableEl = document.getElementById('typesTable');

            // Merge into a combined list
            var schemaMap = {};
            globalSchemas.forEach(function(s) { schemaMap[s] = 'Global'; });
            tenantSchemas.forEach(function(s) { schemaMap[s] = 'Tenant'; });

            var allSchemas = Object.keys(schemaMap);

            if (allSchemas.length === 0) {
                tableEl.innerHTML = '<div class="table-empty">No schemas defined.</div>';
                return;
            }

            var html = '<table><thead><tr><th>Name</th><th>Scope</th></tr></thead><tbody>';
            allSchemas.sort().forEach(function(name) {
                html += '<tr>' +
                    '<td>' + escapeHtml(name) + '</td>' +
                    '<td><span class="badge ' + (schemaMap[name] === 'Global' ? 'badge-live' : 'badge-pending') + '">' + schemaMap[name] + '</span></td>' +
                    '</tr>';
            });
            html += '</tbody></table>';
            tableEl.innerHTML = html;
        } catch (e) {
            document.getElementById('typesTable').innerHTML = '<div class="table-empty">Failed to load schemas.</div>';
        }
    }

    // Content List
    async function renderContentList(container, type) {
        container.innerHTML = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">' +
            '<div><h1 class="view-title">' + escapeHtml(type) + '</h1>' +
            '<p class="view-subtitle">Manage content items</p></div>' +
            '<a href="#/content/' + type + '/new" class="btn btn-primary btn-sm">+ New Item</a></div>' +
            '<div class="table-container"><div class="loading">Loading...</div></div>';

        try {
            var resp = await fetch('/api/content/' + encodeURIComponent(type), {
                headers: { 'X-Tenant': currentTenant }
            });
            var data = await resp.json();
            var items = data.items || [];

            var tableEl = container.querySelector('.table-container');

            if (items.length === 0) {
                tableEl.innerHTML = '<div class="table-empty">No content items found. <a href="#/content/' + type + '/new" class="table-link">Create one</a>.</div>';
                return;
            }

            var html = '<table><thead><tr>' +
                '<th>ID</th><th>Type</th><th>Last Modified</th><th>Size</th><th class="col-actions">Actions</th>' +
                '</tr></thead><tbody>';

            items.forEach(function(item) {
                var modified = item.last_modified ? new Date(item.last_modified).toLocaleDateString() : '-';
                var size = formatSize(item.size || 0);
                var ct = item.content_type || '-';
                html += '<tr>' +
                    '<td><a class="table-link" href="#/content/' + type + '/' + encodeURIComponent(item.id) + '">' + escapeHtml(item.id) + '</a></td>' +
                    '<td class="text-muted text-sm">' + escapeHtml(ct) + '</td>' +
                    '<td class="text-muted text-sm">' + modified + '</td>' +
                    '<td class="text-muted text-sm">' + size + '</td>' +
                    '<td class="col-actions"><div class="table-actions">' +
                        '<a href="#/content/' + type + '/' + encodeURIComponent(item.id) + '" class="action-link">Edit</a>' +
                        '<span class="action-sep">|</span>' +
                        '<button class="action-link danger delete-item-btn" data-id="' + escapeAttr(item.id) + '">Delete</button>' +
                        '<span class="action-sep">|</span>' +
                        '<a href="#/content/' + type + '/' + encodeURIComponent(item.id) + '/history" class="action-link">History</a>' +
                    '</div></td></tr>';
            });

            html += '</tbody></table>';
            tableEl.innerHTML = html;

            // Wire delete buttons
            tableEl.querySelectorAll('.delete-item-btn').forEach(function(btn) {
                btn.addEventListener('click', function() {
                    confirmDelete(type, this.dataset.id, function() {
                        renderContentList(container, type);
                    });
                });
            });
        } catch (e) {
            container.querySelector('.table-container').innerHTML = '<div class="table-empty">Failed to load content.</div>';
        }
    }

    // New Item
    function renderNewItem(container, type) {
        container.innerHTML = '<div class="breadcrumb">' +
            '<a href="#/content/' + type + '">' + escapeHtml(type) + '</a>' +
            '<span class="separator">/</span>' +
            '<span class="current">New Item</span></div>' +
            '<h1 class="view-title" style="margin-top:12px;">Create New Item</h1>' +
            '<p class="view-subtitle">Add a new content item to ' + escapeHtml(type) + '</p>' +
            '<div class="form-group" style="max-width:400px;margin-bottom:16px;">' +
                '<label for="newItemId">Item ID</label>' +
                '<input type="text" id="newItemId" placeholder="my-item-id">' +
            '</div>' +
            '<div id="editorMount"></div>';

        Editor.render(document.getElementById('editorMount'), {}, async function(data) {
            var id = document.getElementById('newItemId').value.trim();
            if (!id) {
                showToast('Please enter an item ID', 'error');
                return;
            }

            try {
                var resp = await fetch('/api/content/' + encodeURIComponent(type) + '/' + encodeURIComponent(id), {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Tenant': currentTenant
                    },
                    body: JSON.stringify(data)
                });

                if (resp.ok) {
                    showToast('Item created successfully', 'success');
                    window.location.hash = '#/content/' + type + '/' + id;
                } else {
                    var err = await resp.json();
                    showToast(err.message || 'Failed to create item', 'error');
                }
            } catch (e) {
                showToast('Failed to create item', 'error');
            }
        });
    }

    // Edit Item
    async function renderEditItem(container, type, id, opts) {
        container.innerHTML = '<div class="breadcrumb">' +
            '<a href="#/content/' + type + '">' + escapeHtml(type) + '</a>' +
            '<span class="separator">/</span>' +
            '<span class="current">' + escapeHtml(id) + '</span></div>' +
            '<div style="display:flex;justify-content:space-between;align-items:center;margin-top:12px;margin-bottom:16px;">' +
            '<h1 class="view-title">' + escapeHtml(id) + '</h1>' +
            '<button class="history-toggle" id="historyToggle">' +
                '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><polyline points="12 6 12 12 16 14"></polyline></svg>' +
                'History</button></div>' +
            '<div id="historyPanel"></div>' +
            '<div id="historyBanner"></div>' +
            '<div id="editorMount"><div class="loading">Loading...</div></div>';

        var historyOpen = false;
        var viewingVersion = null;

        var toggleBtn = document.getElementById('historyToggle');
        var editorMount = document.getElementById('editorMount');
        var historyBanner = document.getElementById('historyBanner');

        toggleBtn.addEventListener('click', function() {
            historyOpen = !historyOpen;
            this.classList.toggle('active', historyOpen);
            if (historyOpen) {
                editorMount.style.display = 'none';
                historyBanner.innerHTML = '';
                viewingVersion = null;
                loadVersionHistory();
            } else {
                document.getElementById('historyPanel').innerHTML = '';
                historyBanner.innerHTML = '';
                viewingVersion = null;
                editorMount.style.display = '';
                loadCurrentContent();
            }
        });

        if (opts && opts.showHistory) {
            historyOpen = true;
            toggleBtn.classList.add('active');
            editorMount.style.display = 'none';
            loadVersionHistory();
        }

        async function loadVersionHistory() {
            var panel = document.getElementById('historyPanel');
            panel.innerHTML = '<div class="history-panel"><div class="loading" style="padding:16px;">Loading versions...</div></div>';

            try {
                var resp = await fetch('/api/content/' + encodeURIComponent(type) + '/' + encodeURIComponent(id) + '/versions', {
                    headers: { 'X-Tenant': currentTenant }
                });

                if (!resp.ok) {
                    panel.innerHTML = '<div class="history-panel"><div class="table-empty">Failed to load version history.</div></div>';
                    return;
                }

                var data = await resp.json();
                var versions = data.versions || [];

                if (versions.length === 0) {
                    panel.innerHTML = '<div class="history-panel"><div class="table-empty">No version history available.</div></div>';
                    return;
                }

                var html = '<div class="history-panel">' +
                    '<div class="history-header"><span>Version History</span><span class="text-muted">' + versions.length + ' version' + (versions.length !== 1 ? 's' : '') + '</span></div>' +
                    '<table><thead><tr>' +
                    '<th>Version</th><th>Date</th><th>Size</th><th class="col-actions">Actions</th>' +
                    '</tr></thead><tbody>';

                versions.forEach(function(v) {
                    var shortId = v.version_id ? v.version_id.substring(0, 12) : '-';
                    var date = v.last_modified ? new Date(v.last_modified).toLocaleString() : '-';
                    var size = formatSize(v.size || 0);
                    var isCurrent = v.is_latest;
                    var rowClass = isCurrent ? ' class="version-current"' : '';

                    html += '<tr' + rowClass + '>' +
                        '<td><span class="version-id">' + escapeHtml(shortId) + '</span>' +
                            (isCurrent ? ' <span class="badge badge-live">Current</span>' : '') + '</td>' +
                        '<td class="text-muted text-sm">' + date + '</td>' +
                        '<td class="text-muted text-sm">' + size + '</td>' +
                        '<td class="col-actions"><div class="version-actions">' +
                            '<button class="action-link view-version-btn" data-version="' + escapeAttr(v.version_id) + '">View</button>' +
                            (isCurrent ? '' : '<span class="action-sep">|</span><button class="action-link restore-version-btn" data-version="' + escapeAttr(v.version_id) + '">Restore</button>') +
                        '</div></td></tr>';
                });

                html += '</tbody></table></div>';
                panel.innerHTML = html;

                // Wire view buttons
                panel.querySelectorAll('.view-version-btn').forEach(function(btn) {
                    btn.addEventListener('click', function() {
                        viewVersion(this.dataset.version);
                    });
                });

                // Wire restore buttons
                panel.querySelectorAll('.restore-version-btn').forEach(function(btn) {
                    btn.addEventListener('click', function() {
                        restoreVersion(this.dataset.version);
                    });
                });
            } catch (e) {
                panel.innerHTML = '<div class="history-panel"><div class="table-empty">Failed to load version history.</div></div>';
            }
        }

        async function viewVersion(versionId) {
            viewingVersion = versionId;
            var shortId = versionId.substring(0, 12);
            historyBanner.innerHTML = '<div class="history-viewing-banner">' +
                '<span>Viewing version ' + escapeHtml(shortId) + ' (read-only)</span>' +
                '<button id="exitVersionView">Close preview</button></div>';

            historyBanner.querySelector('#exitVersionView').addEventListener('click', function() {
                viewingVersion = null;
                historyBanner.innerHTML = '';
            });

            historyBanner.innerHTML += '<div id="versionPreview"><div class="loading">Loading version...</div></div>';
            var preview = document.getElementById('versionPreview');

            try {
                var resp = await fetch('/api/content/' + encodeURIComponent(type) + '/' + encodeURIComponent(id) + '/versions/' + encodeURIComponent(versionId), {
                    headers: {
                        'Accept': 'application/json',
                        'X-Tenant': currentTenant
                    }
                });

                if (!resp.ok) {
                    preview.innerHTML = '<div class="table-empty">Failed to load this version.</div>';
                    return;
                }

                var ct = resp.headers.get('Content-Type') || '';
                if (!ct.includes('json')) {
                    var text = await resp.text();
                    preview.innerHTML = '<div class="editor-container"><div class="editor-body">' +
                        '<pre style="padding:16px;background:var(--color-input-bg);border:1px solid var(--color-border);border-radius:var(--radius-sm);overflow:auto;white-space:pre-wrap;">' +
                        escapeHtml(text) + '</pre></div></div>';
                    return;
                }

                var data = await resp.json();
                var json = JSON.stringify(data, null, 2);
                preview.innerHTML = '<div class="editor-container">' +
                    '<div class="editor-toolbar"><div class="editor-tabs">' +
                        '<span class="editor-tab active" style="cursor:default;">Read-Only Preview</span>' +
                    '</div></div>' +
                    '<div class="editor-body">' +
                        '<textarea class="raw-editor" readonly style="opacity:0.8;">' + escapeHtml(json) + '</textarea>' +
                    '</div></div>';
            } catch (e) {
                preview.innerHTML = '<div class="table-empty">Failed to load this version.</div>';
            }
        }

        async function restoreVersion(versionId) {
            try {
                var resp = await fetch('/api/content/' + encodeURIComponent(type) + '/' + encodeURIComponent(id) + '/versions/' + encodeURIComponent(versionId) + '/restore', {
                    method: 'POST',
                    headers: { 'X-Tenant': currentTenant }
                });

                if (resp.ok) {
                    showToast('Version restored successfully', 'success');
                    // Close history and return to editor
                    historyOpen = false;
                    viewingVersion = null;
                    toggleBtn.classList.remove('active');
                    document.getElementById('historyPanel').innerHTML = '';
                    historyBanner.innerHTML = '';
                    editorMount.style.display = '';
                    loadCurrentContent();
                } else {
                    var err = await resp.json();
                    showToast(err.message || 'Failed to restore version', 'error');
                }
            } catch (e) {
                showToast('Failed to restore version', 'error');
            }
        }

        async function loadCurrentContent() {
            var mount = document.getElementById('editorMount');
            mount.innerHTML = '<div class="loading">Loading...</div>';

            try {
                var resp = await fetch('/api/content/' + encodeURIComponent(type) + '/' + encodeURIComponent(id), {
                    headers: {
                        'Accept': 'application/json',
                        'X-Tenant': currentTenant
                    }
                });

                if (!resp.ok) {
                    mount.innerHTML = '<div class="table-empty">Item not found or cannot be displayed.</div>';
                    return;
                }

                var respContentType = resp.headers.get('Content-Type') || '';

                if (respContentType.startsWith('image/')) {
                    var blob = await resp.blob();
                    var url = URL.createObjectURL(blob);
                    mount.innerHTML =
                        '<div class="media-preview">' +
                            '<div class="media-info text-muted text-sm" style="margin-bottom:12px;">' +
                                escapeHtml(respContentType) +
                            '</div>' +
                            '<img src="' + url + '" alt="' + escapeAttr(id) + '" style="max-width:100%;border-radius:var(--radius);border:1px solid var(--color-border);">' +
                        '</div>';
                    return;
                }

                if (!respContentType.includes('json')) {
                    var text = await resp.text();
                    mount.innerHTML =
                        '<div class="media-preview">' +
                            '<div class="media-info text-muted text-sm" style="margin-bottom:12px;">' +
                                escapeHtml(respContentType) +
                            '</div>' +
                            '<pre style="padding:16px;background:var(--color-input-bg);border:1px solid var(--color-border);border-radius:var(--radius);overflow:auto;white-space:pre-wrap;">' +
                                escapeHtml(text) +
                            '</pre>' +
                        '</div>';
                    return;
                }

                var data = await resp.json();

                Editor.render(mount, data, async function(updated) {
                    try {
                        var saveResp = await fetch('/api/content/' + encodeURIComponent(type) + '/' + encodeURIComponent(id), {
                            method: 'PUT',
                            headers: {
                                'Content-Type': 'application/json',
                                'X-Tenant': currentTenant
                            },
                            body: JSON.stringify(updated)
                        });

                        if (saveResp.ok) {
                            showToast('Item saved successfully', 'success');
                        } else {
                            var err = await saveResp.json();
                            showToast(err.message || 'Failed to save', 'error');
                        }
                    } catch (e) {
                        showToast('Failed to save item', 'error');
                    }
                });
            } catch (e) {
                mount.innerHTML = '<div class="table-empty">Failed to load item.</div>';
            }
        }

        // Initial load
        loadCurrentContent();
    }

    // Delete confirmation
    function confirmDelete(type, id, callback) {
        var overlay = document.createElement('div');
        overlay.className = 'confirm-overlay';
        overlay.innerHTML = '<div class="confirm-dialog">' +
            '<h3>Delete Item</h3>' +
            '<p>Are you sure you want to delete <strong>' + escapeHtml(id) + '</strong>? This action cannot be undone.</p>' +
            '<div class="confirm-dialog-actions">' +
                '<button class="btn btn-ghost btn-sm" id="confirmCancel">Cancel</button>' +
                '<button class="btn btn-danger btn-sm" id="confirmDelete">Delete</button>' +
            '</div></div>';

        document.body.appendChild(overlay);

        overlay.querySelector('#confirmCancel').addEventListener('click', function() {
            overlay.remove();
        });

        overlay.addEventListener('click', function(e) {
            if (e.target === overlay) overlay.remove();
        });

        overlay.querySelector('#confirmDelete').addEventListener('click', async function() {
            try {
                await fetch('/api/content/' + encodeURIComponent(type) + '/' + encodeURIComponent(id), {
                    method: 'DELETE',
                    headers: { 'X-Tenant': currentTenant }
                });
                showToast('Item deleted', 'success');
                if (callback) callback();
            } catch (e) {
                showToast('Failed to delete item', 'error');
            }
            overlay.remove();
        });
    }

    // Toast notification
    function showToast(message, type) {
        var existing = document.querySelector('.toast');
        if (existing) existing.remove();

        var toast = document.createElement('div');
        toast.className = 'toast ' + (type || '');
        toast.textContent = message;
        document.body.appendChild(toast);

        setTimeout(function() { toast.remove(); }, 3000);
    }

    // Utilities
    function formatSize(bytes) {
        if (bytes === 0) return '0 B';
        var k = 1024;
        var sizes = ['B', 'KB', 'MB', 'GB'];
        var i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
    }

    function escapeHtml(str) {
        var div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    function escapeAttr(str) {
        return String(str).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    // Create dialog (reuses confirm-overlay pattern)
    function showCreateDialog(title, placeholder, onSubmit) {
        var overlay = document.createElement('div');
        overlay.className = 'confirm-overlay';
        overlay.innerHTML = '<div class="confirm-dialog">' +
            '<h3>' + escapeHtml(title) + '</h3>' +
            '<div class="form-group" style="margin-bottom:20px;">' +
                '<input type="text" id="createDialogInput" placeholder="' + escapeAttr(placeholder) + '" autocomplete="off">' +
            '</div>' +
            '<div id="createDialogError" class="form-error" style="display:none;"></div>' +
            '<div class="confirm-dialog-actions">' +
                '<button class="btn btn-ghost btn-sm" id="createDialogCancel">Cancel</button>' +
                '<button class="btn btn-primary btn-sm" id="createDialogSubmit">Create</button>' +
            '</div></div>';

        document.body.appendChild(overlay);

        var input = overlay.querySelector('#createDialogInput');
        var errorEl = overlay.querySelector('#createDialogError');
        input.focus();

        overlay.querySelector('#createDialogCancel').addEventListener('click', function() {
            overlay.remove();
        });

        overlay.addEventListener('click', function(e) {
            if (e.target === overlay) overlay.remove();
        });

        input.addEventListener('keydown', function(e) {
            if (e.key === 'Enter') doSubmit();
            if (e.key === 'Escape') overlay.remove();
        });

        overlay.querySelector('#createDialogSubmit').addEventListener('click', doSubmit);

        async function doSubmit() {
            var value = input.value.trim();
            if (!value) {
                errorEl.textContent = 'Name is required';
                errorEl.style.display = '';
                return;
            }
            if (!/^[a-zA-Z0-9-]+$/.test(value)) {
                errorEl.textContent = 'Only letters, numbers, and hyphens allowed';
                errorEl.style.display = '';
                return;
            }
            errorEl.style.display = 'none';
            try {
                await onSubmit(value);
                overlay.remove();
            } catch (e) {
                errorEl.textContent = e.message || 'Operation failed';
                errorEl.style.display = '';
            }
        }
    }

    return { init: init };
})();
