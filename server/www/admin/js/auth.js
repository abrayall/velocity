// Auth module - login, logout, session management, and authenticated fetch
var Auth = (function() {
    async function login(username, password) {
        var resp = await fetch('/api/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username: username, password: password })
        });
        var data = await resp.json();
        if (resp.ok) {
            return { ok: true, token: data.token };
        }
        return { ok: false, message: data.message || 'Login failed' };
    }

    async function logout() {
        await fetch('/api/logout', { method: 'POST' });
        window.location.href = '/admin/login';
    }

    async function checkSession() {
        try {
            var resp = await fetch('/api/session');
            var data = await resp.json();
            return data.valid === true;
        } catch (e) {
            return false;
        }
    }

    // Authenticated fetch wrapper - redirects to login on 401/302
    async function apiFetch(url, options) {
        options = options || {};
        options.credentials = 'same-origin';
        var resp = await fetch(url, options);
        if (resp.status === 401 || resp.redirected) {
            window.location.href = '/admin/login';
            return null;
        }
        return resp;
    }

    return {
        login: login,
        logout: logout,
        checkSession: checkSession,
        fetch: apiFetch
    };
})();
