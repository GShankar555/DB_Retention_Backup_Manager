document.addEventListener('DOMContentLoaded', () => {
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
      } finally { button.disabled = false; button.textContent = 'Test connection'; }
    });
  });

  document.querySelectorAll('.test-connection-form').forEach((button) => {
    button.addEventListener('click', async () => {
      button.disabled = true;
      button.textContent = 'Testing…';
      try {
        const result = await requestJSON(`/api/connections/${button.dataset.connectionId}/test`, { method: 'POST' });
        toast(result.message);
      } catch (error) { toast(error.message, 'error'); }
      finally { button.disabled = false; button.textContent = 'Test saved connection'; }
    });
  });

  const newConnectionTest = document.querySelector('#test-new-connection');
  if (newConnectionTest) {
    newConnectionTest.addEventListener('click', async () => {
      const form = document.querySelector('#connection-form');
      newConnectionTest.disabled = true;
      newConnectionTest.textContent = 'Testing…';
      try {
        const data = Object.fromEntries(new FormData(form).entries());
        const result = await requestJSON('/api/connections/test', { method: 'POST', body: JSON.stringify(data) });
        toast(result.message);
      } catch (error) { toast(error.message, 'error'); }
      finally { newConnectionTest.disabled = false; newConnectionTest.textContent = 'Test connection'; }
    });
  }

  const dryRunButton = document.querySelector('#dry-run-button');
  if (dryRunButton) {
    dryRunButton.addEventListener('click', async () => {
      dryRunButton.disabled = true;
      dryRunButton.textContent = 'Previewing…';
      try {
        const result = await requestJSON('/api/retention/dry-run', { method: 'POST' });
        toast(result.message);
      } catch (error) { toast(error.message, 'error'); }
      finally { dryRunButton.disabled = false; dryRunButton.textContent = '▤  Run dry run'; }
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
    if (cadence.value === 'Daily') return `${minute} ${hour} * * *`;
    if (cadence.value === 'Weekly') return `${minute} ${hour} * * ${weekday}`;
    if (cadence.value === 'Biweekly') return `${minute} ${hour} ${day} */2 *`;
    return `${minute} ${hour} ${day} * *`;
  };
  const refreshCron = () => { if (!manualToggle.checked) cronPreview.textContent = getCron(); };
  [cadence, runDate, runTime].forEach((input) => input.addEventListener('change', refreshCron));
  document.querySelectorAll('input[name="job_type"]').forEach((input) => input.addEventListener('change', () => { description.textContent = descriptions[input.value]; }));
  manualToggle.addEventListener('change', () => { manualInput.disabled = !manualToggle.checked; if (!manualToggle.checked) { manualInput.value = getCron(); cronPreview.textContent = manualInput.value; } else { manualInput.focus(); } });
  manualInput.addEventListener('input', () => { cronPreview.textContent = manualInput.value || '—'; });
  refreshCron();
});
