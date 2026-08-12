// Render document.html -> branded PDF using puppeteer's bundled Chromium.
const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

const BRAND_PRIMARY = '#0E7490';
const BRAND_SLATE = '#334155';
const logoSvg = fs.readFileSync('header.html', 'utf8').trim();

(async () => {
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-gpu'],
  });
  const page = await browser.newPage();
  const url = 'file://' + path.resolve('document.html');
  await page.goto(url, { waitUntil: 'networkidle0', timeout: 120000 });

  const header = `
    <div style="width:100%;font-size:8pt;padding:0 16mm;margin-top:6mm;
         display:flex;align-items:center;justify-content:space-between;
         border-bottom:1.5px solid ${BRAND_PRIMARY};padding-bottom:3mm;">
      <div style="transform:scale(0.9);transform-origin:left center;">${logoSvg}</div>
      <span style="color:${BRAND_SLATE};font-size:7.5pt;">AIOps Intelligence Engine &mdash; Solution Design</span>
    </div>`;

  const footer = `
    <div style="width:100%;font-size:7pt;color:#9aa3a0;padding:0 16mm;margin-bottom:4mm;
         display:flex;justify-content:space-between;align-items:center;">
      <span style="color:${BRAND_PRIMARY};font-weight:700;">Confidential &mdash; Internal Use Only</span>
      <span>Page <span class="pageNumber"></span> of <span class="totalPages"></span></span>
    </div>`;

  await page.pdf({
    path: '../AIOps_Engine_Solution_Design.pdf',
    format: 'A4',
    printBackground: true,
    displayHeaderFooter: true,
    headerTemplate: header,
    footerTemplate: footer,
    margin: { top: '26mm', bottom: '16mm', left: '0', right: '0' },
  });
  await browser.close();
  console.log('PDF written: ../AIOps_Engine_Solution_Design.pdf');
})().catch(e => { console.error(e); process.exit(1); });
