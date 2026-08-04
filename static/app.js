document.addEventListener('DOMContentLoaded', () => {
  const theme = document.documentElement;
  const themeButtons = document.querySelectorAll('[data-theme-toggle]');
  const setTheme = (nextTheme) => {
    theme.dataset.theme = nextTheme;
    localStorage.setItem('vaultline-theme', nextTheme);
  };
  themeButtons.forEach((button) => button.addEventListener('click', () => {
    setTheme(theme.dataset.theme === 'dark' ? 'light' : 'dark');
  }));

  document.querySelectorAll('.local-time').forEach((node) => {
    const date = new Date(node.dataset.utc || '');
    if (!Number.isNaN(date.getTime())) {
      node.textContent = date.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'medium' });
      node.title = `Stored as ${node.dataset.utc} (UTC)`;
    }
  });

  const toast = (message, tone = 'success') => {
    const current = document.querySelector('.client-toast');
    if (current) current.remove();
    const node = document.createElement('div');
    node.className = `flash client-toast ${tone}`;
    node.innerHTML = `<span>${tone === 'success' ? '✓' : '!'}</span>`;
    node.append(document.createTextNode(message));
    document.body.append(node);
    window.setTimeout(() => node.remove(), 3400);
  };

  const requestJSON = async (url, options = {}) => {
    const response = await fetch(url, { headers: { Accept: 'application/json', ...(options.body ? { 'Content-Type': 'application/json' } : {}) }, ...options });
    let data = {};
    try { data = await response.json(); } catch (_error) { data = { message: await response.text() }; }
    if (!response.ok) throw new Error(data.message || `Request failed (${response.status})`);
    return data;
  };

  const searchButton = document.querySelector('#search-button');
  const searchDrawer = document.querySelector('#search-drawer');
  const closeSearch = document.querySelector('#close-search');
  const searchInput = document.querySelector('#global-search');
  const searchResults = document.querySelector('#search-results');
  if (searchButton && searchDrawer) {
    searchButton.addEventListener('click', () => { searchDrawer.hidden = false; searchInput.focus(); });
    closeSearch.addEventListener('click', () => { searchDrawer.hidden = true; });
    let searchTimer;
    searchInput.addEventListener('input', () => {
      window.clearTimeout(searchTimer);
      const query = searchInput.value.trim();
      if (!query) { searchResults.innerHTML = ''; return; }
      searchTimer = window.setTimeout(async () => {
        try {
          const data = await requestJSON(`/api/search?q=${encodeURIComponent(query)}`);
          const items = [
            ...data.jobs.map((item) => `<a href="/jobs/${item.id}/edit"><b>Job</b>${item.name}<small>${item.connection_name}</small></a>`),
            ...data.connections.map((item) => `<a href="/connections"><b>DB</b>${item.name}<small>${item.engine} · ${item.database_name}</small></a>`),
          ];
          searchResults.innerHTML = items.length ? items.join('') : '<p>No matching records.</p>';
        } catch (error) { searchResults.innerHTML = `<p>${error.message}</p>`; }
      }, 220);
    });
  }

  document.querySelectorAll('.test-connection').forEach((button) => {
    button.addEventListener('click', async () => {
      const id = button.dataset.connectionId;
      const status = document.querySelector(`#connection-status-${id}`);
      const idleContent = button.innerHTML;
      button.disabled = true;
      button.textContent = 'Testing…';
      try {
        const result = await requestJSON(`/api/connections/${id}/test`, { method: 'POST' });
        status.className = 'status healthy';
        status.innerHTML = '<i></i>Verified';
        toast(result.message);
      } catch (error) {
        status.className = 'status review';
        status.innerHTML = '<i></i>Failed';
        toast(error.message, 'error');
      } finally { button.disabled = false; button.innerHTML = idleContent; }
    });
  });

  document.querySelectorAll('.test-connection-form').forEach((button) => {
    button.addEventListener('click', async () => {
      const idleContent = button.innerHTML;
      button.disabled = true;
      button.textContent = 'Testing…';
      try {
        const result = await requestJSON(`/api/connections/${button.dataset.connectionId}/test`, { method: 'POST' });
        toast(result.message);
      } catch (error) { toast(error.message, 'error'); }
      finally { button.disabled = false; button.innerHTML = idleContent; }
    });
  });

  const newConnectionTest = document.querySelector('#test-new-connection');
  if (newConnectionTest) {
    newConnectionTest.addEventListener('click', async () => {
      const form = document.querySelector('#connection-form');
      const idleContent = newConnectionTest.innerHTML;
      newConnectionTest.disabled = true;
      newConnectionTest.textContent = 'Testing…';
      try {
        const data = Object.fromEntries(new FormData(form).entries());
        const result = await requestJSON('/api/connections/test', { method: 'POST', body: JSON.stringify(data) });
        toast(result.message);
      } catch (error) { toast(error.message, 'error'); }
      finally { newConnectionTest.disabled = false; newConnectionTest.innerHTML = idleContent; }
    });
  }

  const dryRunButton = document.querySelector('#dry-run-button');
  if (dryRunButton) {
    dryRunButton.addEventListener('click', async () => {
      const idleContent = dryRunButton.innerHTML;
      dryRunButton.disabled = true;
      dryRunButton.textContent = 'Previewing…';
      try {
        const result = await requestJSON('/api/retention/dry-run', { method: 'POST' });
        toast(result.message);
      } catch (error) { toast(error.message, 'error'); }
      finally { dryRunButton.disabled = false; dryRunButton.innerHTML = idleContent; }
    });
  }

  const r2Form = document.querySelector('#r2-form');
  if (r2Form) {
    r2Form.addEventListener('submit', async (event) => {
      event.preventDefault();
      const button = r2Form.querySelector('button');
      button.disabled = true;
      try {
        const data = Object.fromEntries(new FormData(r2Form).entries());
        const result = await requestJSON('/api/settings/r2', { method: 'POST', body: JSON.stringify(data) });
        toast(`R2 target ${result.r2_bucket ? 'saved' : 'cleared'}.`);
      } catch (error) { toast(error.message, 'error'); }
      finally { button.disabled = false; }
    });
  }

  const syncScheduler = document.querySelector('#sync-scheduler');
  if (syncScheduler) {
    syncScheduler.addEventListener('click', async () => {
      const idleContent = syncScheduler.innerHTML;
      syncScheduler.disabled = true;
      syncScheduler.textContent = 'Syncing…';
      try {
        const result = await requestJSON('/api/system/scheduler/sync', { method: 'POST' });
        toast(result.message);
        window.setTimeout(() => window.location.reload(), 650);
      } catch (error) { toast(error.message, 'error'); }
      finally { syncScheduler.disabled = false; syncScheduler.innerHTML = idleContent; }
    });
  }

  document.querySelectorAll('.run-job').forEach((button) => {
    button.addEventListener('click', async () => {
      const jobId = button.dataset.jobId;
      const idleContent = button.innerHTML;
      button.disabled = true;
      button.textContent = 'Starting…';
      try {
        const result = await requestJSON(`/api/jobs/${jobId}/run`, { method: 'POST' });
        toast(result.message);
        window.setTimeout(() => { window.location.href = `/jobs/${jobId}/runs`; }, 450);
      } catch (error) {
        toast(error.message, 'error');
        button.disabled = false;
        button.innerHTML = idleContent;
      }
    });
  });

  const historyJobId = document.body.dataset.runHistoryJob;
  const refreshHistory = document.querySelector('#refresh-history');
  if (refreshHistory) {
    refreshHistory.addEventListener('click', () => {
      refreshHistory.disabled = true;
      refreshHistory.innerHTML = 'Refreshing…';
      window.location.reload();
    });
  }
  if (historyJobId) {
    const statusClass = (status) => status === 'success' ? 'healthy' : status === 'failed' ? 'review' : status === 'running' ? 'running' : 'neutral-status';
    const statusLabel = (status) => status.replace('_', ' ').replace(/\b\w/g, (letter) => letter.toUpperCase());
    const storedUtc = (value) => new Date(`${String(value || '').replace(' ', 'T').replace(/Z$/, '')}Z`);
    const freshnessLabel = (run) => {
      const updated = storedUtc(run.updated_at || run.started_at);
      const ageSeconds = Math.max(0, Math.floor((Date.now() - updated.getTime()) / 1000));
      const age = ageSeconds < 60 ? `${ageSeconds}s` : `${Math.floor(ageSeconds / 60)}m`;
      if (run.status === 'running' && ageSeconds > 120) return `No heartbeat for ${age} · worker may be stalled`;
      if (run.status === 'running') return `Live · updated ${age} ago`;
      if (run.status === 'success') return `Succeeded · updated ${age} ago`;
      if (run.status === 'failed') return `Failed · updated ${age} ago`;
      return `Updated ${age} ago`;
    };
    const refreshRunCard = (card, current) => {
      const status = card.querySelector('.status');
      const percent = card.querySelector('.run-card-top strong');
      const bar = card.querySelector('.progress-track span');
      const message = card.querySelector('.run-message');
      const freshness = card.querySelector('[data-run-freshness]');
      const meta = card.querySelector('.run-state-meta');
      status.className = `status ${statusClass(current.status)}`;
      status.innerHTML = `<i></i>${statusLabel(current.status)}`;
      percent.textContent = `${current.progress}%`;
      bar.style.width = `${current.progress}%`;
      message.textContent = current.message;
      if (freshness) {
        const updated = `${String(current.updated_at || current.started_at).replace(' ', 'T')}Z`;
        freshness.dataset.updatedUtc = updated;
        freshness.className = `run-freshness ${current.status === 'running' ? 'live' : current.status === 'success' ? 'complete' : current.status === 'failed' ? 'error' : ''}`;
        freshness.textContent = freshnessLabel(current);
      }
      if (meta) {
        const autoRefresh = meta.querySelector('.run-auto-refresh');
        if (current.status === 'running' && !autoRefresh) meta.insertAdjacentHTML('beforeend', '<span class="run-auto-refresh">Auto-refreshing</span>');
        if (current.status !== 'running' && autoRefresh) autoRefresh.remove();
      }
    };
    const pollRun = async () => {
      try {
        const result = await requestJSON(`/api/jobs/${historyJobId}/runs`);
        const current = result.runs[0];
        const card = document.querySelector('.run-card');
        if (!current || !card) return;
        refreshRunCard(card, current);
      } catch (_error) {
        const freshness = document.querySelector('[data-run-freshness]');
        if (freshness) freshness.textContent = 'Status check failed · retrying…';
      }
    };
    pollRun();
    window.setInterval(pollRun, 2000);
  }

  const form = document.querySelector('#job-form');
  if (!form) return;
  const cadence = document.querySelector('#cadence');
  const runDate = document.querySelector('#run-date');
  const runTime = document.querySelector('#run-time');
  const cronPreview = document.querySelector('#cron-preview');
  const manualToggle = document.querySelector('#manual-cron');
  const manualInput = document.querySelector('.manual-cron-input');
  const description = document.querySelector('#operation-description');
  const descriptions = {
    backup: 'Create a complete database backup and upload it to Cloudflare R2.',
    retention: 'Delete old records from the selected tables based on a retention window.',
    archive: 'Write old rows to Parquet in R2, verify the upload, then remove them.'
  };
  const getCron = () => {
    const date = new Date(`${runDate.value}T12:00:00`);
    const [hour, minute] = (runTime.value || '00:00').split(':').map(Number);
    const day = date.getDate() || 1;
    const weekday = date.getDay();
    if (cadence.value === 'Hourly') return `${minute} * * * *`;
    if (cadence.value === 'Daily') return `${minute} ${hour} * * *`;
    if (cadence.value === 'Weekly') return `${minute} ${hour} * * ${weekday}`;
    if (cadence.value === 'Biweekly') return `${minute} ${hour} * * ${weekday}`;
    return `${minute} ${hour} ${day} * *`;
  };
  const refreshCron = () => { if (!manualToggle.checked) cronPreview.textContent = getCron(); };
  [cadence, runDate, runTime].forEach((input) => input.addEventListener('change', refreshCron));
  document.querySelectorAll('input[name="job_type"]').forEach((input) => input.addEventListener('change', () => { description.textContent = descriptions[input.value]; }));
  manualToggle.addEventListener('change', () => { manualInput.disabled = !manualToggle.checked; if (!manualToggle.checked) { manualInput.value = getCron(); cronPreview.textContent = manualInput.value; } else { manualInput.focus(); } });
  manualInput.addEventListener('input', () => { cronPreview.textContent = manualInput.value || '—'; });
  refreshCron();
});
