// Repository owner changes belong here; HTML links remain useful without JavaScript.
const repositoryURL = 'https://github.com/olup/porcupine';
for (const link of document.querySelectorAll('[data-repo-path]')) {
  link.href = repositoryURL + link.dataset.repoPath;
}

const example = document.querySelector('[data-examples]');
const tabs = [...example.querySelectorAll('[role="tab"]')];
const panels = tabs.map(tab => document.getElementById(tab.getAttribute('aria-controls')));
const status = example.querySelector('.copy-status');
const copyButton = example.querySelector('.copy-button');
let selected = 0;
let statusTimer;

function selectTab(index, focus = false) {
  selected = index;
  tabs.forEach((tab, position) => {
    tab.setAttribute('aria-selected', String(position === index));
    tab.tabIndex = position === index ? 0 : -1;
    panels[position].hidden = position !== index;
  });
  status.textContent = '';
  clearTimeout(statusTimer);
  copyButton.textContent = 'Copy';
  copyButton.setAttribute('aria-label', `Copy ${['capture request', 'SQL query', 'webhook pipeline request'][index]}`);
  if (focus) tabs[index].focus();
}

for (const [index, tab] of tabs.entries()) {
  tab.addEventListener('click', () => selectTab(index));
  tab.addEventListener('keydown', event => {
    const next = { ArrowRight: (index + 1) % tabs.length, ArrowLeft: (index - 1 + tabs.length) % tabs.length, Home: 0, End: tabs.length - 1 }[event.key];
    if (next === undefined) return;
    event.preventDefault();
    selectTab(next, true);
  });
}

copyButton.addEventListener('click', async () => {
  const code = panels[selected].querySelector('code');
  try {
    await navigator.clipboard.writeText(code.textContent);
    status.textContent = 'Copied';
    copyButton.textContent = 'Copied';
  } catch {
    const selection = window.getSelection();
    const range = document.createRange();
    range.selectNodeContents(code);
    selection.removeAllRanges();
    selection.addRange(range);
    status.textContent = 'Code selected. Use your copy shortcut.';
    copyButton.textContent = 'Selected';
  }
  statusTimer = setTimeout(() => { status.textContent = ''; copyButton.textContent = 'Copy'; }, 2400);
});

example.querySelector('.tabs').hidden = false;
example.querySelector('.copy-controls').hidden = false;
selectTab(0);
