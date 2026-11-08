const promptText = document.getElementById('prompt');
const copy = document.getElementById('copy');
const status = document.getElementById('copy-status');
let reset;

copy.hidden = false;
copy.addEventListener('click', async () => {
  clearTimeout(reset);
  try {
    await navigator.clipboard.writeText(promptText.textContent);
    copy.textContent = 'Copied';
    status.textContent = 'Prompt copied to clipboard.';
  } catch {
    const range = document.createRange();
    range.selectNodeContents(promptText);
    const selection = window.getSelection();
    selection.removeAllRanges();
    selection.addRange(range);
    copy.textContent = 'Text selected';
    status.textContent = 'Prompt selected. Use your copy shortcut.';
  }
  reset = setTimeout(() => {
    copy.textContent = 'Copy prompt';
    status.textContent = '';
  }, 2200);
});
