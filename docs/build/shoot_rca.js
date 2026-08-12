const puppeteer = require('puppeteer');
(async () => {
  const browser = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox','--disable-gpu'] });
  const page = await browser.newPage();
  await page.setViewport({ width: 1100, height: 1400, deviceScaleFactor: 1.5 });
  const errors = [];
  page.on('console', m => { if (m.type() === 'error') errors.push(m.text()); });
  page.on('pageerror', e => errors.push('PAGEERROR: ' + e.message));
  await page.goto('http://localhost:8000/', { waitUntil: 'domcontentloaded', timeout: 60000 });
  // Load RCA data + switch to RCA tab via the app's own functions.
  await page.evaluate(async () => { if (window.loadRCA) await window.loadRCA(); if (window.showTab) window.showTab('rca'); });
  await new Promise(r => setTimeout(r, 2500));
  const cards = await page.evaluate(() => document.querySelectorAll('#rca-page-body .panel').length);
  const sections = await page.evaluate(() => Array.from(document.querySelectorAll('#rca-page-body .rca-section-title')).slice(0,12).map(e => e.textContent.trim()));
  await page.screenshot({ path: 'rca_page.png', fullPage: false });
  console.log('RCA cards rendered:', cards);
  console.log('First sections:', JSON.stringify(sections));
  console.log('Console errors:', errors.length ? JSON.stringify(errors.slice(0,5)) : 'none');
  await browser.close();
})().catch(e => { console.error(e); process.exit(1); });
