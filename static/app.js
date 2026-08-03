document.addEventListener('DOMContentLoaded', () => {
  const searchButton = document.querySelector('#search-button');
  const searchDrawer = document.querySelector('#search-drawer');
  const closeSearch = document.querySelector('#close-search');
  if (searchButton && searchDrawer) {
    searchButton.addEventListener('click', () => { searchDrawer.hidden = false; searchDrawer.querySelector('input').focus(); });
    closeSearch.addEventListener('click', () => { searchDrawer.hidden = true; });
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
    if (cadence.value === 'Weekly') return `${minute} ${hour} * * ${weekday === 0 ? 0 : weekday}`;
    if (cadence.value === 'Biweekly') return `${minute} ${hour} ${day} */2 *`;
    return `${minute} ${hour} ${day} * * *`.replace(' * * *', ' * *');
  };
  const refreshCron = () => { if (!manualToggle.checked) cronPreview.textContent = getCron(); };
  [cadence, runDate, runTime].forEach((input) => input.addEventListener('change', refreshCron));
  document.querySelectorAll('input[name="job_type"]').forEach((input) => input.addEventListener('change', () => { description.textContent = descriptions[input.value]; }));
  manualToggle.addEventListener('change', () => { manualInput.disabled = !manualToggle.checked; if (!manualToggle.checked) { manualInput.value = getCron(); cronPreview.textContent = manualInput.value; } else { manualInput.focus(); } });
  manualInput.addEventListener('input', () => { cronPreview.textContent = manualInput.value || '—'; });
  refreshCron();
});
